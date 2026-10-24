package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/state"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeGroupSession struct {
	begins                          int
	ends                            []kgo.TransactionEndTry
	rows                            []*kgo.Record
	committed                       bool
	produceErr, commitErr, abortErr error
	onProduce                       func()
}

func (s *fakeGroupSession) Begin() error { s.begins++; return nil }
func (s *fakeGroupSession) ProduceSync(_ context.Context, rows ...*kgo.Record) kgo.ProduceResults {
	results := make(kgo.ProduceResults, len(rows))
	for i, row := range rows {
		copy := *row
		copy.Key = record.CloneBytes(row.Key)
		copy.Value = record.CloneBytes(row.Value)
		s.rows = append(s.rows, &copy)
		results[i] = kgo.ProduceResult{Record: row, Err: s.produceErr}
	}
	if s.onProduce != nil {
		s.onProduce()
	}
	return results
}
func (s *fakeGroupSession) End(_ context.Context, commit kgo.TransactionEndTry) (bool, error) {
	s.ends = append(s.ends, commit)
	if commit == kgo.TryAbort {
		return false, s.abortErr
	}
	return s.committed, s.commitErr
}
func groupBackendFixture() (*groupBackend, *fakeGroupSession, []execution.TaskTransaction) {
	session := &fakeGroupSession{committed: true}
	assignment := &groupAssignment{valid: true, partitions: map[int32]bool{0: true, 1: true}}
	tracker := &assignmentTracker{topics: []string{"in", "right"}}
	tracker.current.Store(assignment)
	b := &groupBackend{config: Config{ChangelogTopic: "state"}, application: "app", session: session, tracker: tracker, assignment: assignment, entries: make(map[int32]groupTaskCheckpoint), pollNext: map[int32]map[string]int64{0: {"in": 1}, 1: {"in": 1}}}
	txs := make([]execution.TaskTransaction, 2)
	for i := range txs {
		p := int32(i)
		spec := execution.Spec{ApplicationID: "app", ID: "0", Partition: p}
		b.entries[p] = groupTaskCheckpoint{generation: uint64(i + 1), checkpoint: execution.Checkpoint{NextOffsets: map[string]int64{"in": 0, "right": 7}, StreamTime: 1}, barrier: encodeKey(barrierKind, spec, "", "marker")}
		txs[i] = execution.TaskTransaction{Spec: spec, Generation: uint64(i + 1), Transaction: execution.Transaction{Mutations: []state.Mutation{{Store: "s", Key: "k", Value: []byte("value")}}, Outputs: []record.Bytes{{Topic: "out", Partition: p, Key: []byte("k"), Value: []byte("v")}}, NextOffsets: map[string]int64{"in": 1}, StreamTime: 5}}
	}
	return b, session, txs
}

func TestGroupBackendFullCheckpointsAndBarrierCleanup(t *testing.T) {
	b, session, txs := groupBackendFixture()
	if err := b.CommitTasks(context.Background(), txs); err != nil {
		t.Fatal(err)
	}
	if session.begins != 1 || !reflect.DeepEqual(session.ends, []kgo.TransactionEndTry{kgo.TryCommit}) || len(session.rows) != 8 {
		t.Fatal("group durable decision split or omitted records", session.begins, session.ends, len(session.rows))
	}
	for _, entry := range b.entries {
		if entry.barrier != nil || !reflect.DeepEqual(entry.checkpoint.NextOffsets, map[string]int64{"in": 1, "right": 7}) || entry.checkpoint.StreamTime != 5 {
			t.Fatal("checkpoint dropped untouched topic or barrier", entry)
		}
	}
	barriers := 0
	for _, row := range session.rows {
		if row.Topic == "state" {
			key, err := decodeKey(row.Key)
			if err != nil {
				t.Fatal(err)
			}
			if key.kind == barrierKind {
				barriers++
				if row.Value != nil {
					t.Fatal("barrier cleanup was not tombstone")
				}
			}
		}
	}
	if barriers != 2 {
		t.Fatal("restore barriers not removed", barriers)
	}
}

func TestGroupBackendRejectsIncompletePollAndStaleTaskBeforeProduce(t *testing.T) {
	for _, mode := range []string{"omitted", "stale", "duplicate", "foreign"} {
		t.Run(mode, func(t *testing.T) {
			b, session, txs := groupBackendFixture()
			switch mode {
			case "omitted":
				txs = txs[:1]
			case "stale":
				txs[1].Generation++
			case "duplicate":
				txs[1] = txs[0]
			case "foreign":
				txs[1].Spec.ApplicationID = "other"
			}
			if err := b.CommitTasks(context.Background(), txs); err == nil {
				t.Fatal("unsafe group checkpoint accepted")
			}
			if len(session.rows) != 0 || !reflect.DeepEqual(session.ends, []kgo.TransactionEndTry{kgo.TryAbort}) {
				t.Fatal("unsafe transaction reached production", session.ends)
			}
			for _, entry := range b.entries {
				if entry.checkpoint.NextOffsets["in"] != 0 {
					t.Fatal("failed group published checkpoint")
				}
			}
		})
	}
}

func TestGroupBackendRebalanceAndAmbiguousErrorsDoNotPublishMetadata(t *testing.T) {
	for _, mode := range []string{"before", "during-produce", "end-abort", "commit-error", "abort-error"} {
		t.Run(mode, func(t *testing.T) {
			b, session, txs := groupBackendFixture()
			failure := errors.New("session failed")
			switch mode {
			case "before":
				b.tracker.current.Store(&groupAssignment{})
			case "during-produce":
				session.onProduce = func() { b.tracker.current.Store(&groupAssignment{}) }
			case "end-abort":
				session.committed = false
			case "commit-error":
				session.commitErr = failure
			case "abort-error":
				b.tracker.current.Store(&groupAssignment{})
				session.abortErr = failure
			}
			err := b.CommitTasks(context.Background(), txs)
			if err == nil {
				t.Fatal("failed session was successful")
			}
			fatal := mode == "commit-error" || mode == "abort-error"
			if fatal && (!errors.Is(err, failure) || errors.Is(err, errGroupRebalanced)) {
				t.Fatal("fatal session failure was treated as retryable rebalance", err)
			}
			if !fatal && !errors.Is(err, errGroupRebalanced) {
				t.Fatal(err)
			}
			for _, entry := range b.entries {
				if entry.checkpoint.NextOffsets["in"] != 0 || entry.barrier == nil {
					t.Fatal("failed decision published metadata")
				}
			}
		})
	}
}

func TestGroupBackendReplayOnlyPollCommitsOffsetsAndCleansBarrier(t *testing.T) {
	b, session, _ := groupBackendFixture()
	for p, entry := range b.entries {
		entry.checkpoint.NextOffsets["in"] = 1
		b.entries[p] = entry
	}
	if err := b.CommitTasks(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(session.rows) != 2 || !reflect.DeepEqual(session.ends, []kgo.TransactionEndTry{kgo.TryCommit}) {
		t.Fatal("replay-only poll could not advance offsets")
	}
}

func TestWorkerEpochFenceNeverCommitsGroupOffsets(t *testing.T) {
	session := &fakeGroupSession{committed: true}
	config := GroupConfig{Kafka: Config{ChangelogTopic: "state"}, ApplicationID: "app", InstanceID: "instance"}
	if err := fenceWorker(context.Background(), session, config); err != nil {
		t.Fatal(err)
	}
	if session.begins != 1 || len(session.rows) != 1 || !reflect.DeepEqual(session.ends, []kgo.TransactionEndTry{kgo.TryAbort}) {
		t.Fatal("bootstrap fence published a transaction", session.ends)
	}
	key, err := decodeKey(session.rows[0].Key)
	if err != nil || key.kind != barrierKind || session.rows[0].Topic != "state" {
		t.Fatal("bootstrap wrote user state/output", err)
	}
}
