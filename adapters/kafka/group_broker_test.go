package kafka

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
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

type runningGroup struct {
	cancel  context.CancelFunc
	done    chan error
	stopped bool
}

func (w *runningGroup) stop(t *testing.T) {
	t.Helper()
	if w.stopped {
		return
	}
	w.cancel()
	select {
	case err := <-w.done:
		w.stopped = true
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Error("group worker failed", err)
		}
	case <-time.After(15 * time.Second):
		t.Error("group worker did not stop")
	}
}
func startTestGroup(t *testing.T, ctx context.Context, config GroupConfig, hook func(record.Record[string, string])) *runningGroup {
	t.Helper()
	engine, err := persistent.Open(filepath.Join(t.TempDir(), "cache"), persistent.Config{SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	})
	builder := stream.NewStreamsBuilder()
	source := stream.Stream(builder, testInput, serde.String, serde.String)
	if hook != nil {
		source = source.Process("test-phase", false, func(_ processor.Context, next processor.Forward[string, string]) processor.Handler[string, string] {
			return func(r record.Record[string, string]) error { hook(r); return next(r) }
		})
	}
	stream.Count(source, "counts").ToStream().To(testOutput)
	plan, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
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
func groupTestConfig(t *testing.T, application, instance string) GroupConfig {
	config := brokerConfig(t)
	config.Options = append(config.Options, kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(200*time.Millisecond), kgo.RebalanceTimeout(10*time.Second))
	return GroupConfig{Kafka: config, ApplicationID: application, InstanceID: instance, Topics: []string{testInput}, BatchSize: 128, PollInterval: 50 * time.Millisecond}
}
func writeGroupInput(t *testing.T, ctx context.Context, producer *kgo.Client, prefix string, perPartition int) map[int32]int64 {
	t.Helper()
	var rows []*kgo.Record
	for partition := int32(0); partition < 3; partition++ {
		for i := 0; i < perPartition; i++ {
			rows = append(rows, &kgo.Record{Topic: testInput, Partition: partition, Key: []byte(fmt.Sprintf("%s/%d", prefix, partition)), Value: []byte("x"), Timestamp: time.Now()})
		}
	}
	if err := producer.ProduceSync(ctx, rows...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	positions := make(map[int32]int64)
	for _, row := range rows {
		positions[row.Partition] = row.Offset + 1
	}
	return positions
}
func groupOutputReader(t *testing.T, config Config) *kgo.Client {
	return clientFor(t, config, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{testOutput: {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(50*time.Millisecond))
}
func awaitGroupCounts(t *testing.T, ctx context.Context, reader *kgo.Client, prefix string, counts map[string]int, want int) {
	t.Helper()
	for {
		total := 0
		for _, count := range counts {
			total += count
		}
		if total >= want {
			return
		}
		fetches := reader.PollRecords(ctx, 256)
		if failures := fetches.Errors(); len(failures) > 0 {
			t.Fatal("group output fetch", failures[0].Err)
		}
		for _, row := range fetches.Records() {
			key := string(row.Key)
			if !strings.HasPrefix(key, prefix+"/") {
				continue
			}
			value, err := serde.Int64.Decode(row.Value)
			if err != nil {
				t.Fatal(err)
			}
			counts[key]++
			if value != int64(counts[key]) {
				t.Fatalf("duplicate/lost group count for %s: value=%d expected=%d", key, value, counts[key])
			}
		}
	}
}

func assertNoExtraGroupOutput(t *testing.T, reader *kgo.Client, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	for {
		fetches := reader.PollRecords(ctx, 256)
		for _, row := range fetches.Records() {
			if strings.HasPrefix(string(row.Key), prefix+"/") {
				t.Fatalf("extra committed group output for %s", row.Key)
			}
		}
		if failures := fetches.Errors(); len(failures) > 0 {
			if errors.Is(failures[0].Err, context.DeadlineExceeded) {
				return
			}
			t.Fatal(failures[0].Err)
		}
	}
}
func awaitCommittedGroupOffsets(t *testing.T, ctx context.Context, client *kgo.Client, group string, want map[int32]int64) {
	t.Helper()
	for {
		request := kmsg.NewPtrOffsetFetchRequest()
		request.RequireStable = true
		entry := kmsg.NewOffsetFetchRequestGroup()
		entry.Group = group
		topic := kmsg.NewOffsetFetchRequestGroupTopic()
		topic.Topic = testInput
		topic.Partitions = []int32{0, 1, 2}
		entry.Topics = []kmsg.OffsetFetchRequestGroupTopic{topic}
		request.Groups = []kmsg.OffsetFetchRequestGroup{entry}
		response, err := request.RequestWith(ctx, client)
		ok := err == nil && response != nil && len(response.Groups) == 1 && response.Groups[0].ErrorCode == 0
		got := make(map[int32]int64)
		if ok {
			for _, topic := range response.Groups[0].Topics {
				for _, partition := range topic.Partitions {
					if partition.ErrorCode != 0 {
						ok = false
					}
					got[partition.Partition] = partition.Offset
				}
			}
		}
		for partition, offset := range want {
			if got[partition] < offset {
				ok = false
			}
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("group offsets did not commit: got=%v want=%v err=%v", got, want, err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
func awaitTwoMembers(t *testing.T, ctx context.Context, client *kgo.Client, group string) {
	t.Helper()
	for {
		request := kmsg.NewPtrDescribeGroupsRequest()
		request.Groups = []string{group}
		response, err := request.RequestWith(ctx, client)
		if err == nil && len(response.Groups) == 1 && kerr.ErrorForCode(response.Groups[0].ErrorCode) == nil && response.Groups[0].State == "Stable" && len(response.Groups[0].Members) == 2 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("second group member did not join", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestBrokerGroupTransactionsRestartAndCommittedOffsets(t *testing.T) {
	base := brokerConfig(t)
	ctx := brokerContext(t)
	application := unique("group-restart")
	producer := clientFor(t, base, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	aborted := clientFor(t, base, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.TransactionalID(unique("aborted-input")))
	if err := aborted.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	writeGroupInput(t, ctx, aborted, application, 1)
	if err := aborted.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	positions := writeGroupInput(t, ctx, producer, application, 3)
	reader := groupOutputReader(t, base)
	counts := make(map[string]int)
	first := startTestGroup(t, ctx, groupTestConfig(t, application, "worker-a"), nil)
	awaitGroupCounts(t, ctx, reader, application, counts, 9)
	awaitCommittedGroupOffsets(t, ctx, producer, application, positions)
	first.stop(t)
	positions = writeGroupInput(t, ctx, producer, application, 2)
	second := startTestGroup(t, ctx, groupTestConfig(t, application, "worker-a"), nil)
	awaitGroupCounts(t, ctx, reader, application, counts, 15)
	awaitCommittedGroupOffsets(t, ctx, producer, application, positions)
	second.stop(t)
	assertNoExtraGroupOutput(t, reader, application)
	for _, count := range counts {
		if count != 5 {
			t.Fatal("restart lost/duplicated group input", counts)
		}
	}
}

func TestBrokerGroupRebalanceDuringStaging(t *testing.T) {
	base := brokerConfig(t)
	ctx := brokerContext(t)
	application := unique("group-rebalance")
	producer := clientFor(t, base, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	positions := writeGroupInput(t, ctx, producer, application, 2)
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	first := startTestGroup(t, ctx, groupTestConfig(t, application, "worker-a"), func(r record.Record[string, string]) {
		if r.Key == application+"/0" && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("first group worker did not reach staged input")
	}
	second := startTestGroup(t, ctx, groupTestConfig(t, application, "worker-b"), nil)
	awaitTwoMembers(t, ctx, producer, application)
	close(release)
	released = true
	reader := groupOutputReader(t, base)
	counts := make(map[string]int)
	awaitGroupCounts(t, ctx, reader, application, counts, 6)
	awaitCommittedGroupOffsets(t, ctx, producer, application, positions)
	first.stop(t)
	second.stop(t)
	assertNoExtraGroupOutput(t, reader, application)
	for _, count := range counts {
		if count != 2 {
			t.Fatal("rebalance lost/duplicated input", counts)
		}
	}
}

// The parent kills this helper after ProduceSync and before End. It intentionally
// has no client cleanup: the broker must retain a genuinely abandoned transaction.
func TestBrokerGroupCrashHelper(t *testing.T) {
	application := os.Getenv("KSTREAM_GROUP_CRASH_HELPER")
	if application == "" {
		t.Skip("subprocess helper only")
	}
	config := brokerConfig(t)
	session, err := kgo.NewGroupTransactSession(kgo.SeedBrokers(config.Brokers...), kgo.ConsumerGroup(application), kgo.ConsumeTopics(testInput), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(200*time.Millisecond), kgo.RebalanceTimeout(10*time.Second),
		kgo.DisableAutoCommit(), kgo.RequireStableFetchOffsets(), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.TransactionalID(workerTransactionID(application, "worker-a")), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	fetches := session.PollRecords(ctx, 1)
	if failures := fetches.Errors(); len(failures) > 0 {
		t.Fatal(failures[0].Err)
	}
	if fetches.NumRecords() == 0 {
		t.Fatal("crash helper did not poll input")
	}
	if err := session.Begin(); err != nil {
		t.Fatal(err)
	}
	stateValue := make([]byte, 16)
	binary.BigEndian.PutUint64(stateValue, 999)
	binary.BigEndian.PutUint64(stateValue[8:], uint64(time.Now().UnixMilli()))
	outputValue, err := serde.Int64.Encode(999)
	if err != nil {
		t.Fatal(err)
	}
	spec := execution.Spec{ApplicationID: application, ID: "0"}
	rows := []*kgo.Record{
		{Topic: testState, Partition: 0, Key: encodeKey(stateKind, spec, "counts", application+"/0"), Value: stateValue, Timestamp: time.Now()},
		{Topic: testOutput, Partition: 0, Key: []byte(application + "/0"), Value: outputValue, Timestamp: time.Now()},
	}
	if err := session.ProduceSync(ctx, rows...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	fmt.Println("KSTREAM_GROUP_TRANSACTION_OPEN")
	select {}
}

func TestBrokerGroupRecoversKilledOpenTransaction(t *testing.T) {
	base := brokerConfig(t)
	ctx := brokerContext(t)
	application := unique("group-killed")
	producer := clientFor(t, base, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	positions := writeGroupInput(t, ctx, producer, application, 1)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, executable, "-test.run=^TestBrokerGroupCrashHelper$", "-test.v")
	child.Env = append(os.Environ(), "KSTREAM_GROUP_CRASH_HELPER="+application)
	pipe, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stderr = child.Stdout
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(pipe)
		var last string
		for scanner.Scan() {
			last = scanner.Text()
			if last == "KSTREAM_GROUP_TRANSACTION_OPEN" {
				ready <- nil
				return
			}
		}
		ready <- fmt.Errorf("crash helper did not open transaction: last=%s err=%v", last, scanner.Err())
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = child.Wait()
	waited = true
	if err == nil {
		t.Fatal("crash helper exited without forced kill")
	}
	// A normal transaction timeout is much longer than this recovery gate.
	// Fencing the stable worker epoch must unblock restoration promptly.
	recoverCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	worker := startTestGroup(t, recoverCtx, groupTestConfig(t, application, "worker-a"), nil)
	reader := groupOutputReader(t, base)
	counts := make(map[string]int)
	awaitGroupCounts(t, recoverCtx, reader, application, counts, 3)
	awaitCommittedGroupOffsets(t, recoverCtx, producer, application, positions)
	worker.stop(t)
	assertNoExtraGroupOutput(t, reader, application)
	for _, count := range counts {
		if count != 1 {
			t.Fatal("abandoned transaction leaked state/output", counts)
		}
	}
}
