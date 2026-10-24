package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	persistent "github.com/SoraDaibu/kstream-go/adapters/pebble"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/state"
	"github.com/SoraDaibu/kstream-go/stream"
	"github.com/twmb/franz-go/pkg/kgo"
)

const testState = "kstream-test-state"
const testOutput = "kstream-test-output"
const testInput = "kstream-test-input"

func brokerConfig(t *testing.T) Config {
	t.Helper()
	raw := os.Getenv("KSTREAM_KAFKA_BROKERS")
	if raw == "" {
		t.Skip("requires KSTREAM_KAFKA_BROKERS and provisioned test topics")
	}
	return Config{Brokers: strings.Split(raw, ","), ChangelogTopic: testState}
}
func backendFor(t *testing.T, config Config) *Backend {
	t.Helper()
	b, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return b
}
func brokerContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func unique(prefix string) string { return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()) }
func clientFor(t *testing.T, config Config, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append(opts, kgo.SeedBrokers(config.Brokers...))
	c, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func brokerStoreOptions(t *testing.T) []execution.Option {
	t.Helper()
	directory := os.Getenv("KSTREAM_KAFKA_STATE_DIR")
	if directory == "" {
		return nil
	}
	engine, err := persistent.Open(filepath.Join(directory, unique("engine")), persistent.Config{SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	})
	return []execution.Option{execution.WithStoreFactory(engine.Task)}
}
func ownTask(t *testing.T, task *execution.Task) {
	t.Helper()
	t.Cleanup(func() {
		if err := task.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestBrokerRecoveryReplayAndReadCommitted(t *testing.T) {
	cfg := brokerConfig(t)
	ctx := brokerContext(t)
	options := brokerStoreOptions(t)
	id := unique("recovery")
	spec := execution.Spec{ApplicationID: id, ID: "0"}
	builder := stream.NewStreamsBuilder()
	stream.Count(stream.Stream(builder, "in", serde.String, serde.String), "counts").ToStream().To(testOutput)
	plan, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	first := backendFor(t, cfg)
	task, err := plan.NewTask(ctx, spec, first, options...)
	if err != nil {
		t.Fatal(err)
	}
	ownTask(t, task)
	now := time.Now().UnixMilli()
	rows := []record.Bytes{{Topic: "in", Offset: 0, Timestamp: now, Key: []byte(id), Value: []byte("x")}, {Topic: "in", Offset: 1, Timestamp: now + 1, Key: []byte(id), Value: []byte("x")}}
	if err = task.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	// Inject an aborted state mutation and output with the correct task scope.
	// Neither may be observed by restoration or a read-committed output reader.
	aborted := clientFor(t, cfg, kgo.TransactionalID(unique("aborted")), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err = aborted.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err = aborted.ProduceSync(ctx, &kgo.Record{Topic: testState, Partition: 0, Key: encodeKey(stateKind, spec, "counts", id), Value: []byte("invalid-aborted-state")}, &kgo.Record{Topic: testOutput, Partition: 0, Key: []byte(id), Value: []byte("aborted-output")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err = aborted.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	if err = task.Close(); err != nil {
		t.Fatal(err)
	}
	first.Close()
	restored := backendFor(t, cfg)
	task, err = plan.NewTask(ctx, spec, restored, options...)
	if err != nil {
		t.Fatal(err)
	}
	ownTask(t, task)
	if err = task.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	last := rows[0]
	last.Offset = 2
	last.Timestamp = now + 2
	if err = task.Process(ctx, last); err != nil {
		t.Fatal(err)
	}
	reader := clientFor(t, cfg, kgo.ConsumeTopics(testOutput), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	var got []int64
	for len(got) < 3 {
		fetches := reader.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatal(errs[0].Err)
		}
		for _, r := range fetches.Records() {
			if string(r.Key) == id {
				value, err := serde.Int64.Decode(r.Value)
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, value)
			}
		}
	}
	if !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatalf("output duplicated or read aborted data: %v", got)
	}
	if restored.NextOffsets()["in"] != 3 {
		t.Fatal("input checkpoint did not advance")
	}
}

func TestBrokerCheckpointRetainsAllTopicsAndTombstones(t *testing.T) {
	cfg := brokerConfig(t)
	ctx := brokerContext(t)
	spec := execution.Spec{ApplicationID: unique("checkpoint"), ID: "0"}
	b := backendFor(t, cfg)
	g, _, err := b.Open(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Commit(ctx, spec, g, execution.Transaction{Mutations: []state.Mutation{{Store: "lookup", Key: "a\x00", Value: []byte("old")}}, NextOffsets: map[string]int64{"left": 4}, StreamTime: 10}); err != nil {
		t.Fatal(err)
	}
	if err = b.Commit(ctx, spec, g, execution.Transaction{Mutations: []state.Mutation{{Store: "lookup", Key: "a\x00", Value: nil}, {Store: "lookup", Key: "empty", Value: []byte{}}}, NextOffsets: map[string]int64{"right": 9}, StreamTime: 20}); err != nil {
		t.Fatal(err)
	}
	b.Close()
	restored := backendFor(t, cfg)
	_, snapshot, err := restored.Open(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot.NextOffsets, map[string]int64{"left": 4, "right": 9}) || snapshot.StreamTime != 20 {
		t.Fatalf("checkpoint: %+v", snapshot)
	}
	if _, ok := snapshot.State["lookup"]["a\x00"]; ok {
		t.Fatal("tombstone was lost")
	}
	value, ok := snapshot.State["lookup"]["empty"]
	if !ok || value == nil || len(value) != 0 {
		t.Fatal("empty value became a tombstone")
	}
}

func TestBrokerFencesOldOwnerAndSeparatesPartitions(t *testing.T) {
	cfg := brokerConfig(t)
	ctx := brokerContext(t)
	spec := execution.Spec{ApplicationID: unique("fencing"), ID: "task"}
	old := backendFor(t, cfg)
	generation, _, err := old.Open(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	otherSpec := spec
	otherSpec.Partition = 1
	other := backendFor(t, cfg)
	if _, _, err = other.Open(ctx, otherSpec); err != nil {
		t.Fatal(err)
	}
	if err = old.Commit(ctx, spec, generation, execution.Transaction{NextOffsets: map[string]int64{"in": 1}, StreamTime: 1}); err != nil {
		t.Fatalf("another partition fenced this owner: %v", err)
	}
	replacement := backendFor(t, cfg)
	if _, _, err = replacement.Open(ctx, spec); err != nil {
		t.Fatal(err)
	}
	err = old.Commit(ctx, spec, generation, execution.Transaction{NextOffsets: map[string]int64{"in": 2}, StreamTime: 2})
	if !errors.Is(err, execution.ErrFenced) {
		t.Fatalf("stale owner was not fenced: %v", err)
	}
}

func TestBrokerExplicitInputResumesAtCommittedCheckpoint(t *testing.T) {
	cfg := brokerConfig(t)
	ctx := brokerContext(t)
	options := brokerStoreOptions(t)
	id := unique("input")
	spec := execution.Spec{ApplicationID: id, ID: "0"}
	producer := clientFor(t, cfg, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: testInput, Partition: 0, Key: []byte(id), Value: []byte("first")}, &kgo.Record{Topic: testInput, Partition: 0, Key: []byte(id), Value: []byte("second")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	setup := func(b *Backend) *execution.Task {
		task, err := execution.New(ctx, spec, b, options...)
		if err != nil {
			t.Fatal(err)
		}
		if err = task.Register(testInput, func(r record.Bytes) error {
			task.Emit(record.Bytes{Topic: testOutput, Key: r.Key, Value: r.Value, Timestamp: r.Timestamp})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		ownTask(t, task)
		return task
	}
	b := backendFor(t, cfg)
	task := setup(b)
	input, err := b.Input(testInput)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := input.Next(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || string(rows[0].Value) != "first" {
		t.Fatalf("initial input %+v", rows)
	}
	if err = task.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	offset := rows[0].Offset
	input.Close()
	if err = task.Close(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	b = backendFor(t, cfg)
	task = setup(b)
	input, err = b.Input(testInput)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	rows, err = input.Next(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Offset != offset+1 || string(rows[0].Value) != "second" {
		t.Fatalf("resumed input %+v", rows)
	}
	if err = task.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
}
