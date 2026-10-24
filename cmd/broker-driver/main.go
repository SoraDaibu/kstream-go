// broker-driver independently verifies and measures committed broker output.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type config struct {
	command, scenario, brokers, application, input, output, changelog string
	records, warmup, keys, partitions                                 int
	timeout                                                           time.Duration
}

func parse() (config, error) {
	var c config
	if len(os.Args) < 2 {
		return c, errors.New("init, run or delete command required")
	}
	c.command = os.Args[1]
	f := flag.NewFlagSet(c.command, flag.ContinueOnError)
	f.StringVar(&c.scenario, "scenario", "count", "filter or count")
	f.StringVar(&c.brokers, "brokers", "localhost:9092", "broker addresses")
	f.StringVar(&c.application, "application", "", "application/group ID")
	f.StringVar(&c.input, "input", "", "source topic")
	f.StringVar(&c.output, "output", "", "output topic")
	f.StringVar(&c.changelog, "changelog", "", "changelog topic")
	f.IntVar(&c.records, "records", 200000, "measured records")
	f.IntVar(&c.warmup, "warmup", 100000, "warmup records")
	f.IntVar(&c.keys, "keys", 64, "uniform keys")
	f.IntVar(&c.partitions, "partitions", 3, "topic partitions")
	f.DurationVar(&c.timeout, "timeout", 180*time.Second, "case deadline")
	if err := f.Parse(os.Args[2:]); err != nil {
		return c, err
	}
	if c.application == "" || c.input == "" || c.output == "" || c.changelog == "" || c.records < 1 || c.warmup < 0 || c.keys < 1 || c.partitions < 1 || c.timeout <= 0 || (c.scenario != "filter" && c.scenario != "count") {
		return c, errors.New("invalid case configuration")
	}
	if c.command != "init" && c.command != "run" && c.command != "delete" {
		return c, errors.New("unknown command")
	}
	return c, nil
}
func topics(ctx context.Context, client *kgo.Client, c config) error {
	names := []string{c.input, c.output, c.changelog}
	if c.command == "delete" {
		request := kmsg.NewPtrDeleteTopicsRequest()
		for _, name := range names {
			name := name
			topic := kmsg.NewDeleteTopicsRequestTopic()
			topic.Topic = &name
			request.Topics = append(request.Topics, topic)
		}
		response, err := request.RequestWith(ctx, client)
		if err != nil {
			return err
		}
		for _, topic := range response.Topics {
			if err := kerr.ErrorForCode(topic.ErrorCode); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
				return err
			}
		}
		return nil
	}
	request := kmsg.NewPtrCreateTopicsRequest()
	for _, name := range names {
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic = name
		topic.NumPartitions = int32(c.partitions)
		topic.ReplicationFactor = 1
		if name == c.changelog {
			policy, retention := "compact", "-1"
			topic.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "cleanup.policy", Value: &policy}, {Name: "retention.ms", Value: &retention}}
		}
		request.Topics = append(request.Topics, topic)
	}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return err
	}
	for _, topic := range response.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			return fmt.Errorf("create %s: %w", topic.Topic, err)
		}
	}
	return nil
}

func awaitTopics(ctx context.Context, client *kgo.Client, c config) error {
	for {
		request := kmsg.NewPtrMetadataRequest()
		request.AllowAutoTopicCreation = false
		for _, name := range []string{c.input, c.output, c.changelog} {
			name := name
			topic := kmsg.NewMetadataRequestTopic()
			topic.Topic = &name
			request.Topics = append(request.Topics, topic)
		}
		response, err := request.RequestWith(ctx, client)
		ready := err == nil && response != nil && len(response.Topics) == 3
		if ready {
			for _, topic := range response.Topics {
				if topic.ErrorCode != 0 || len(topic.Partitions) != c.partitions {
					ready = false
				}
				for _, partition := range topic.Partitions {
					if partition.ErrorCode != 0 || partition.Leader < 0 {
						ready = false
					}
				}
			}
		}
		if ready {
			return nil
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("topics not ready: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func verifyNoExtra(parent context.Context, reader *kgo.Client) error {
	ctx, cancel := context.WithTimeout(parent, 300*time.Millisecond)
	defer cancel()
	for {
		fetches := reader.PollRecords(ctx, 4096)
		if fetches.NumRecords() > 0 {
			return errors.New("unexpected extra committed output")
		}
		if failures := fetches.Errors(); len(failures) > 0 {
			if errors.Is(failures[0].Err, context.DeadlineExceeded) && parent.Err() == nil {
				return nil
			}
			return failures[0].Err
		}
	}
}

type verifier struct {
	scenario string
	seen     []int64
}

func (v *verifier) inputID(row *kgo.Record) (int64, error) {
	key, err := strconv.Atoi(string(row.Key))
	if err != nil || key < 0 || key >= len(v.seen) {
		return 0, errors.New("unexpected output key")
	}
	value := int64(0)
	if v.scenario == "count" {
		value, err = serde.Int64.Decode(row.Value)
	} else {
		value, err = strconv.ParseInt(string(row.Value), 10, 64)
	}
	if err != nil {
		return 0, err
	}
	next := v.seen[key] + 1
	global := (next-1)*int64(len(v.seen)) + int64(key)
	if (v.scenario == "count" && value != next) || (v.scenario == "filter" && value != global) {
		return 0, fmt.Errorf("lost/duplicate/out-of-order output key=%d value=%d expected ordinal=%d", key, value, next)
	}
	v.seen[key] = next
	return global, nil
}
func mix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

type phaseResult struct {
	Records      int    `json:"records"`
	Nanos        int64  `json:"nanos"`
	ProduceNanos int64  `json:"source_produce_nanos"`
	Checksum     string `json:"checksum"`
	P50          int64  `json:"p50_latency_ns"`
	P95          int64  `json:"p95_latency_ns"`
	P99          int64  `json:"p99_latency_ns"`
	Max          int64  `json:"max_latency_ns"`
	err          error
}

func phase(parent context.Context, producer, reader *kgo.Client, c config, v *verifier, origin time.Time, start int64, n int, positions map[int32]int64) phaseResult {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	ledger := make([]atomic.Int64, n)
	latencies := make([]int64, 0, n)
	done := make(chan phaseResult, 1)
	begin := time.Now()
	go func() {
		var checksum uint64
		for len(latencies) < n {
			fetches := reader.PollRecords(ctx, 4096)
			if failures := fetches.Errors(); len(failures) > 0 {
				done <- phaseResult{err: failures[0].Err}
				return
			}
			for _, row := range fetches.Records() {
				id, err := v.inputID(row)
				if err != nil {
					done <- phaseResult{err: err}
					return
				}
				if id < start || id >= start+int64(n) {
					done <- phaseResult{err: errors.New("output outside expected phase")}
					return
				}
				sent := ledger[id-start].Load()
				if sent <= 0 {
					done <- phaseResult{err: errors.New("output has no monotonic send ledger entry")}
					return
				}
				latencies = append(latencies, time.Since(origin).Nanoseconds()-sent)
				checksum += mix(uint64(id))
			}
		}
		elapsed := time.Since(begin).Nanoseconds()
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		quantile := func(p int) int64 {
			if n == 0 {
				return 0
			}
			return latencies[(n-1)*p/100]
		}
		maximum := int64(0)
		if n > 0 {
			maximum = latencies[len(latencies)-1]
		}
		done <- phaseResult{Records: n, Nanos: elapsed, Checksum: fmt.Sprintf("%016x", checksum), P50: quantile(50), P95: quantile(95), P99: quantile(99), Max: maximum}
	}()
	keys := make([][]byte, c.keys)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
	}
	for offset := 0; offset < n; {
		end := offset + 1024
		if end > n {
			end = n
		}
		rows := make([]*kgo.Record, 0, end-offset)
		for i := offset; i < end; i++ {
			id := start + int64(i)
			ledger[i].Store(time.Since(origin).Nanoseconds())
			rows = append(rows, &kgo.Record{Topic: c.input, Key: keys[id%int64(c.keys)], Value: []byte(strconv.FormatInt(id, 10)), Timestamp: time.Now()})
		}
		if err := producer.ProduceSync(ctx, rows...).FirstErr(); err != nil {
			cancel()
			<-done
			return phaseResult{err: err}
		}
		for _, row := range rows {
			positions[row.Partition] = row.Offset + 1
		}
		offset = end
	}
	produceNanos := time.Since(begin).Nanoseconds()
	result := <-done
	result.ProduceNanos = produceNanos
	if produceNanos > result.Nanos {
		result.Nanos = produceNanos
	}
	return result
}
func verifyOffsets(ctx context.Context, client *kgo.Client, c config, want map[int32]int64) error {
	for {
		request := kmsg.NewPtrOffsetFetchRequest()
		request.RequireStable = true
		group := kmsg.NewOffsetFetchRequestGroup()
		group.Group = c.application
		topic := kmsg.NewOffsetFetchRequestGroupTopic()
		topic.Topic = c.input
		for partition := int32(0); partition < int32(c.partitions); partition++ {
			topic.Partitions = append(topic.Partitions, partition)
		}
		group.Topics = []kmsg.OffsetFetchRequestGroupTopic{topic}
		request.Groups = []kmsg.OffsetFetchRequestGroup{group}
		response, err := request.RequestWith(ctx, client)
		if err != nil {
			return err
		}
		valid := len(response.Groups) == 1 && response.Groups[0].ErrorCode == 0
		got := make(map[int32]int64)
		if valid {
			for _, topic := range response.Groups[0].Topics {
				for _, partition := range topic.Partitions {
					if partition.ErrorCode != 0 {
						valid = false
					}
					got[partition.Partition] = partition.Offset
				}
			}
		}
		for partition, offset := range want {
			if got[partition] != offset {
				valid = false
			}
		}
		if valid {
			return nil
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("group offsets incomplete: want=%v got=%v: %w", want, got, ctx.Err())
		case <-timer.C:
		}
	}
}
func run() error {
	c, err := parse()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(c.brokers, ",")...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return err
	}
	defer producer.Close()
	if c.command != "run" {
		if err := topics(ctx, producer, c); err != nil {
			return err
		}
		if c.command == "init" {
			return awaitTopics(ctx, producer, c)
		}
		return nil
	}
	assigned := make(map[int32]kgo.Offset, c.partitions)
	for p := int32(0); p < int32(c.partitions); p++ {
		assigned[p] = kgo.NewOffset().AtStart()
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(c.brokers, ",")...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{c.output: assigned}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(50*time.Millisecond), kgo.FetchMaxBytes(8<<20), kgo.MaxConcurrentFetches(1))
	if err != nil {
		return err
	}
	defer reader.Close()
	v := &verifier{scenario: c.scenario, seen: make([]int64, c.keys)}
	positions := make(map[int32]int64)
	origin := time.Now()
	warm := phase(ctx, producer, reader, c, v, origin, 0, c.warmup, positions)
	if warm.err != nil {
		return warm.err
	}
	encoder := json.NewEncoder(os.Stdout)
	if err := encoder.Encode(map[string]any{"event": "warmup_done", "result": warm}); err != nil {
		return err
	}
	result := phase(ctx, producer, reader, c, v, origin, int64(c.warmup), c.records, positions)
	if result.err != nil {
		return result.err
	}
	if err := encoder.Encode(map[string]any{"event": "measurement_output_done"}); err != nil {
		return err
	}
	if err := verifyOffsets(ctx, producer, c, positions); err != nil {
		return err
	}
	if err := verifyNoExtra(ctx, reader); err != nil {
		return err
	}
	return encoder.Encode(map[string]any{"event": "measurement_done", "result": result, "group_offsets_verified": true, "source_offsets": positions})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
