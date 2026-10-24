package execution_test

import (
	"context"
	"errors"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/state"
)

type streamingBackend struct {
	legacyCalls, commits int
	restore              func(context.Context, execution.RestoreSink) (execution.Checkpoint, error)
}

func (b *streamingBackend) Open(context.Context, execution.Spec) (uint64, execution.Snapshot, error) {
	b.legacyCalls++
	return 0, execution.Snapshot{}, errors.New("legacy image must not be requested")
}
func (b *streamingBackend) OpenInto(ctx context.Context, _ execution.Spec, sink execution.RestoreSink) (uint64, execution.Checkpoint, error) {
	checkpoint, err := b.restore(ctx, sink)
	return 7, checkpoint, err
}
func (b *streamingBackend) Commit(context.Context, execution.Spec, uint64, execution.Transaction) error {
	b.commits++
	return nil
}

func TestTaskSelectsStreamingRestoreAndOwnsBorrowedValues(t *testing.T) {
	positions := map[string]int64{"in": 10}
	backend := &streamingBackend{restore: func(_ context.Context, sink execution.RestoreSink) (execution.Checkpoint, error) {
		value := []byte("one")
		for _, row := range []state.Mutation{
			{Store: "store", Key: "kept", Value: value},
			{Store: "store", Key: "deleted", Value: value},
			{Store: "store", Key: "deleted", Value: nil},
			{Store: "store", Key: "empty", Value: []byte{}},
		} {
			if err := sink([]state.Mutation{row}); err != nil {
				return execution.Checkpoint{}, err
			}
		}
		value[0] = 'X'
		return execution.Checkpoint{NextOffsets: positions, StreamTime: 42}, nil
	}}
	task, err := execution.New(ctx, execution.Spec{ApplicationID: "streamed", ID: "0"}, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	positions["in"] = 99 // Task checkpoint must be independently owned.
	value, ok, err := task.StoreGet("store", "kept")
	if err != nil || !ok || string(value) != "one" {
		t.Fatal("borrowed restore value escaped", value, ok, err)
	}
	if _, ok, err = task.StoreGet("store", "deleted"); err != nil || ok {
		t.Fatal("restore lost ordered tombstone")
	}
	value, ok, err = task.StoreGet("store", "empty")
	if err != nil || !ok || value == nil || len(value) != 0 {
		t.Fatal("restore lost empty value")
	}
	if backend.legacyCalls != 0 || task.StreamTime() != 42 {
		t.Fatal("streamed metadata not installed")
	}
	calls := 0
	if err := task.Register("in", func(record.Bytes) error { calls++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := task.ProcessBatch(ctx, []record.Bytes{{Topic: "in", Offset: 9}, {Topic: "in", Offset: 10}}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || backend.commits != 1 {
		t.Fatal("streamed checkpoint replay suppression failed")
	}
}

type restoreFailureStore struct {
	*state.Memory
	applies, closes int
	resetFailure    bool
}

func (s *restoreFailureStore) Reset() error {
	if s.resetFailure {
		return injected
	}
	return s.Memory.Reset()
}
func (s *restoreFailureStore) ApplyBatch(rows []state.Mutation) error {
	s.applies++
	if s.applies == 2 {
		return injected
	}
	return s.Memory.ApplyBatch(rows)
}
func (s *restoreFailureStore) Close() error { s.closes++; return s.Memory.Close() }

func TestStreamingRestoreFailureClosesUnpublishedStore(t *testing.T) {
	for _, reset := range []bool{false, true} {
		storage := &restoreFailureStore{Memory: state.NewMemory(), resetFailure: reset}
		opens := 0
		backend := &streamingBackend{restore: func(_ context.Context, sink execution.RestoreSink) (execution.Checkpoint, error) {
			opens++
			for i := 0; i < 2; i++ {
				if err := sink([]state.Mutation{{Store: "store", Key: "key", Value: []byte("partial")}}); err != nil {
					return execution.Checkpoint{}, err
				}
			}
			return execution.Checkpoint{}, nil
		}}
		task, err := execution.New(ctx, execution.Spec{ApplicationID: "partial", ID: "0"}, backend,
			execution.WithStoreFactory(func(execution.Spec) (state.Store, error) { return storage, nil }))
		if task != nil || !errors.Is(err, injected) || storage.closes != 1 || backend.legacyCalls != 0 || backend.commits != 0 {
			t.Fatal("partial restore published or leaked", task, err, storage.closes)
		}
		if reset && opens != 0 {
			t.Fatal("failed reset still revoked backend ownership")
		}
	}
}
