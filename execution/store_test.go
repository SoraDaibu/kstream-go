package execution_test

import (
	"context"
	"errors"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/state"
)

type faultStore struct {
	*state.Memory
	readFailure, scanFailure, writeFailure bool
	closes                                 int
}

func (s *faultStore) Get(store, key string) ([]byte, bool, error) {
	if s.readFailure {
		return nil, false, injected
	}
	return s.Memory.Get(store, key)
}
func (s *faultStore) Scan(store string, visit func(string, []byte)) error {
	if s.scanFailure {
		return injected
	}
	return s.Memory.Scan(store, visit)
}
func (s *faultStore) ApplyBatch(rows []state.Mutation) error {
	if s.writeFailure && len(rows) > 0 {
		return injected
	}
	return s.Memory.ApplyBatch(rows)
}
func (s *faultStore) Close() error { s.closes++; return s.Memory.Close() }
func TestIgnoredStoreErrorsStillAbortBeforeBackendCommit(t *testing.T) {
	for _, scan := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "scan"}[scan], func(t *testing.T) {
			storage := &faultStore{Memory: state.NewMemory()}
			backend := &recordingBackend{Backend: memory.New()}
			task, err := execution.New(ctx, execution.Spec{ApplicationID: "fault", ID: "0"}, backend, execution.WithStoreFactory(func(execution.Spec) (state.Store, error) { return storage, nil }))
			if err != nil {
				t.Fatal(err)
			}
			storage.readFailure = !scan
			storage.scanFailure = scan
			if err = task.Register("in", func(record.Bytes) error {
				if scan {
					_ = task.StoreScan("store", func(string, []byte) {})
				} else {
					_, _, _ = task.StoreGet("store", "key")
				}
				task.Emit(record.Bytes{Topic: "out", Value: []byte("must-not-commit")})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err = task.Process(ctx, record.Bytes{Topic: "in"}); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			if backend.commits != 0 {
				t.Fatal("ignored read error committed invalid output")
			}
			if err = task.Process(ctx, record.Bytes{Topic: "in"}); !errors.Is(err, execution.ErrPoisoned) {
				t.Fatal(err)
			}
			if err = task.Close(); err != nil {
				t.Fatal(err)
			}
			if storage.closes != 1 {
				t.Fatal("store ownership not released")
			}
		})
	}
}
func TestLocalWriteFailureAfterBackendCommitRestoresWithoutDuplicateOutput(t *testing.T) {
	storage := &faultStore{Memory: state.NewMemory()}
	log := memory.New()
	spec := execution.Spec{ApplicationID: "local-apply", ID: "0"}
	setup := func(options ...execution.Option) *execution.Task {
		task, err := execution.New(ctx, spec, log, options...)
		if err != nil {
			t.Fatal(err)
		}
		if err = task.Register("in", func(r record.Bytes) error {
			task.StorePut("store", "key", r.Value)
			task.Emit(record.Bytes{Topic: "out", Value: r.Value})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return task
	}
	task := setup(execution.WithStoreFactory(func(execution.Spec) (state.Store, error) { return storage, nil }))
	storage.writeFailure = true
	input := record.Bytes{Topic: "in", Value: []byte("committed")}
	if err := task.Process(ctx, input); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if len(log.Outputs(spec)) != 1 {
		t.Fatal("backend did not commit before cache failure")
	}
	if _, _, err := task.StoreGet("store", "key"); !errors.Is(err, execution.ErrPoisoned) {
		t.Fatal("failed cache remained queryable", err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	task = setup()
	defer task.Close()
	if err := task.Process(ctx, input); err != nil {
		t.Fatal(err)
	}
	if len(log.Outputs(spec)) != 1 {
		t.Fatal("recovery duplicated committed output")
	}
	value, ok, err := task.StoreGet("store", "key")
	if err != nil || !ok || string(value) != "committed" {
		t.Fatalf("restored state %s %v %v", value, ok, err)
	}
}
func TestStoreFactoryFailureDoesNotFenceExistingTask(t *testing.T) {
	backend := memory.New()
	spec := execution.Spec{ApplicationID: "factory", ID: "0"}
	original, err := execution.New(ctx, spec, backend)
	if err != nil {
		t.Fatal(err)
	}
	if err = original.Register("in", func(record.Bytes) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = execution.New(ctx, spec, backend, execution.WithStoreFactory(func(execution.Spec) (state.Store, error) { return nil, injected })); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if err = original.Process(ctx, record.Bytes{Topic: "in"}); err != nil {
		t.Fatal("setup failure revoked healthy owner", err)
	}
}
func TestTaskCloseRejectsActiveProcessingAndIsIdempotent(t *testing.T) {
	storage := &faultStore{Memory: state.NewMemory()}
	task, err := execution.New(ctx, execution.Spec{ApplicationID: "close", ID: "0"}, memory.New(), execution.WithStoreFactory(func(execution.Spec) (state.Store, error) { return storage, nil }))
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	if err = task.Register("in", func(record.Bytes) error { close(entered); <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	go func() { done <- task.Process(context.Background(), record.Bytes{Topic: "in"}) }()
	<-entered
	if err = task.Close(); !errors.Is(err, execution.ErrConcurrent) {
		t.Fatal(err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = task.Close(); err != nil {
		t.Fatal(err)
	}
	if err = task.Close(); err != nil {
		t.Fatal(err)
	}
	if storage.closes != 1 {
		t.Fatal("store closed more than once")
	}
	if err = task.Process(ctx, record.Bytes{Topic: "in", Offset: 1}); !errors.Is(err, execution.ErrClosed) {
		t.Fatal(err)
	}
}
