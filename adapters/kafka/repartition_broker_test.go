package kafka

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	persistent "github.com/SoraDaibu/kstream-go/adapters/pebble"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func provisionExchangeTestTopics(t *testing.T, ctx context.Context, admin *kgo.Client, policies map[string]string) {
	t.Helper()
	request := kmsg.NewPtrCreateTopicsRequest()
	for name, policy := range policies {
		value := policy
		request.Topics = append(request.Topics, kmsg.CreateTopicsRequestTopic{
			Topic: name, NumPartitions: 3, ReplicationFactor: 1,
			Configs: []kmsg.CreateTopicsRequestTopicConfig{{Name: "cleanup.policy", Value: &value}},
		})
	}
	response, err := request.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range response.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			t.Fatal("create exchange test topic", topic.Topic, err)
		}
	}
	metadata := kmsg.NewPtrMetadataRequest()
	metadata.AllowAutoTopicCreation = false
	for name := range policies {
		topic := name
		metadata.Topics = append(metadata.Topics, kmsg.MetadataRequestTopic{Topic: &topic})
	}
	for {
		response, err := metadata.RequestWith(ctx, admin)
		ready := err == nil && response != nil && len(response.Topics) == len(policies)
		if ready {
			for _, topic := range response.Topics {
				ready = ready && topic.ErrorCode == 0 && len(topic.Partitions) == 3
				for _, partition := range topic.Partitions {
					ready = ready && partition.ErrorCode == 0
				}
			}
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("created exchange topics did not become ready", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		request := kmsg.NewPtrDeleteTopicsRequest()
		for name := range policies {
			topic := name
			request.TopicNames = append(request.TopicNames, name)
			request.Topics = append(request.Topics, kmsg.DeleteTopicsRequestTopic{Topic: &topic})
		}
		if _, err := request.RequestWith(cleanup, admin); err != nil {
			t.Error("delete owned exchange test topics", err)
		}
	})
}

func TestBrokerRepartitionRejectsCompactedExchange(t *testing.T) {
	base := brokerConfig(t)
	ctx := brokerContext(t)
	name := unique("compacted-exchange")
	admin := clientFor(t, base)
	provisionExchangeTestTopics(t, ctx, admin, map[string]string{name: "compact"})
	config := groupTestConfig(t, unique("reject-exchange"), "worker")
	config.Kafka.RepartitionTopics = []string{name}
	config.Topics = append(config.Topics, name)
	err := RunGroup(ctx, config, func(context.Context, execution.Spec, execution.Backend) (*execution.Task, error) {
		t.Error("unsafe exchange policy reached task construction")
		return nil, errors.New("unexpected task construction")
	})
	if err == nil || !strings.Contains(err.Error(), "cleanup.policy=delete") {
		t.Fatal("compaction could silently remove exchange updates", err)
	}
}

func startExchangeTestGroup(t *testing.T, ctx context.Context, config GroupConfig, plan stream.Topology) *runningGroup {
	t.Helper()
	engine, err := persistent.Open(filepath.Join(t.TempDir(), "exchange-cache"), persistent.Config{SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	})
	workerCtx, cancel := context.WithCancel(ctx)
	w := &runningGroup{cancel: cancel, done: make(chan error, 1)}
	go func() {
		w.done <- RunGroup(workerCtx, config, func(ctx context.Context, spec execution.Spec, backend execution.Backend) (*execution.Task, error) {
			return plan.NewTask(ctx, spec, backend, execution.WithStoreFactory(engine.Task))
		})
	}()
	t.Cleanup(func() { w.stop(t) })
	return w
}

func awaitExchangeOffset(t *testing.T, ctx context.Context, admin *kgo.Client, application, topic string, partition int32, minimum int64) {
	t.Helper()
	for {
		request := kmsg.NewPtrOffsetFetchRequest()
		request.RequireStable = true
		request.Groups = []kmsg.OffsetFetchRequestGroup{{Group: application,
			Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: topic, Partitions: []int32{partition}}}}}
		response, err := request.RequestWith(ctx, admin)
		if err == nil && len(response.Groups) == 1 && response.Groups[0].ErrorCode == 0 {
			for _, row := range response.Groups[0].Topics {
				for _, p := range row.Partitions {
					if p.ErrorCode == 0 && p.Offset >= minimum {
						return
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("exchange group position was not committed", topic, partition, minimum, err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestBrokerRepartitionCrossPartitionRecoveryAndAbortedExchange(t *testing.T) {
	base := brokerConfig(t)
	ctx := brokerContext(t)
	application := unique("repartition")
	source, output, changelog := application+"-input", application+"-output", application+"-state"
	keyed := kgo.StickyKeyPartitioner(nil).ForTopic("exchange")
	key, partition := "", 0
	for i := 0; partition == 0; i++ {
		key = fmt.Sprintf("route/%d", i)
		partition = keyed.Partition(&kgo.Record{Key: []byte(key)}, 3)
	}
	builder := stream.NewStreamsBuilder()
	changed := stream.SelectKey(stream.Stream(builder, source, serde.String, serde.String), "key", serde.String,
		func(record.Record[string, string]) (string, bool) { return key, false })
	var clocks []int64
	downstream := changed.Repartition("exchange").Process("clock", false,
		func(c processor.Context, next processor.Forward[string, string]) processor.Handler[string, string] {
			return func(r record.Record[string, string]) error { clocks = append(clocks, c.StreamTime()); return next(r) }
		})
	stream.Count(downstream, "counts").ToStream().To(output)
	plan, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	topics, err := plan.InputTopics(application)
	if err != nil {
		t.Fatal(err)
	}
	exchanges, err := plan.RepartitionTopics(application)
	if err != nil || len(exchanges) != 1 {
		t.Fatal(exchanges, err)
	}
	admin := clientFor(t, base)
	provisionExchangeTestTopics(t, ctx, admin, map[string]string{source: "delete", output: "delete", changelog: "compact", exchanges[0]: "delete"})
	config := GroupConfig{
		Kafka:         Config{Brokers: base.Brokers, ChangelogTopic: changelog, RepartitionTopics: exchanges, OutputPartitioner: kgo.ManualPartitioner()},
		ApplicationID: application, InstanceID: "worker", Topics: topics, BatchSize: 128, PollInterval: 50 * time.Millisecond,
		Options: []kgo.Opt{kgo.SessionTimeout(6 * time.Second), kgo.HeartbeatInterval(200 * time.Millisecond), kgo.RebalanceTimeout(10 * time.Second)},
	}
	producer := clientFor(t, base, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	reader := clientFor(t, base, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{output: {
		0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart(),
	}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(50*time.Millisecond))
	exchangeReader := clientFor(t, base, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{exchanges[0]: {
		int32(partition): kgo.NewOffset().AtStart(),
	}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(50*time.Millisecond))
	stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	write := func(first, count int) int64 {
		var rows []*kgo.Record
		for i := first; i < first+count; i++ {
			rows = append(rows, &kgo.Record{Topic: source, Partition: 0, Key: []byte(fmt.Sprintf("old-%d", i)), Value: []byte("x"), Timestamp: stamp.Add(time.Duration(i%8) * time.Millisecond)})
		}
		if err := producer.ProduceSync(ctx, rows...).FirstErr(); err != nil {
			t.Fatal(err)
		}
		return rows[len(rows)-1].Offset + 1
	}
	seen, exchanged := 0, 0
	lastExchange := int64(-1)
	await := func(want int) {
		for seen < want {
			fetch := reader.PollRecords(ctx, 128)
			if failures := fetch.Errors(); len(failures) > 0 {
				t.Fatal(failures[0].Err)
			}
			for _, row := range fetch.Records() {
				seen++
				value, err := serde.Int64.Decode(row.Value)
				if err != nil || string(row.Key) != key || row.Partition != int32(partition) || value != int64(seen) || !row.Timestamp.Equal(stamp.Add(time.Duration(min(seen-1, 7))*time.Millisecond)) {
					t.Fatal("lost routing/count/timestamp or replayed exchange", row, value, seen, err)
				}
			}
		}
		for exchanged < want {
			fetch := exchangeReader.PollRecords(ctx, 128)
			if failures := fetch.Errors(); len(failures) > 0 {
				t.Fatal(failures[0].Err)
			}
			for _, row := range fetch.Records() {
				if string(row.Key) != key || string(row.Value) != "x" || row.Partition != int32(partition) {
					t.Fatal("aborted or corrupt exchange became visible", row)
				}
				exchanged++
				lastExchange = row.Offset + 1
			}
		}
	}
	first := startExchangeTestGroup(t, ctx, config, plan)
	next := write(0, 8)
	await(8)
	awaitExchangeOffset(t, ctx, admin, application, source, 0, next)
	awaitExchangeOffset(t, ctx, admin, application, exchanges[0], int32(partition), lastExchange)
	first.stop(t)
	aborted := clientFor(t, base, kgo.TransactionalID(application+"-aborted"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err := aborted.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.ProduceSync(ctx, &kgo.Record{Topic: exchanges[0], Partition: int32(partition), Key: []byte(key), Value: []byte("ghost")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	second := startExchangeTestGroup(t, ctx, config, plan) // Fresh Pebble cache forces changelog restore.
	next = write(8, 8)
	await(16)
	awaitExchangeOffset(t, ctx, admin, application, source, 0, next)
	awaitExchangeOffset(t, ctx, admin, application, exchanges[0], int32(partition), lastExchange)
	second.stop(t)
	if len(clocks) != 16 {
		t.Fatal("missing processor clocks", clocks)
	}
	for i, clock := range clocks {
		if want := stamp.Add(time.Duration(min(i, 7)) * time.Millisecond).UnixMilli(); clock != want {
			t.Fatal("downstream processing clock did not restore before late input", i, clock, want)
		}
	}
	if seen != 16 || exchanged != 16 {
		t.Fatal("unexpected exchange/output totals", seen, exchanged)
	}
	tail, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	for {
		fetch := reader.PollRecords(tail, 128)
		if len(fetch.Records()) > 0 {
			t.Fatal("extra committed repartition output")
		}
		if failures := fetch.Errors(); len(failures) > 0 {
			if errors.Is(failures[0].Err, context.DeadlineExceeded) {
				break
			}
			t.Fatal(failures[0].Err)
		}
	}
}
