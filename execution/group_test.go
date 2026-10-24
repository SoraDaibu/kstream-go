package execution_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/state"
)

type atomicProbe struct {
	*memory.Backend
	calls, empty          int
	failBefore, failAfter bool
}

func (b *atomicProbe) CommitTasks(ctx context.Context, tx []execution.TaskTransaction) error {
	b.calls++
	if len(tx) == 0 {
		b.empty++
	}
	if b.failBefore {
		return injected
	}
	if err := b.Backend.CommitTasks(ctx, tx); err != nil {
		return err
	}
	if b.failAfter {
		return injected
	}
	return nil
}
func groupSpec(part int) execution.Spec {
	return execution.Spec{ApplicationID: "group-test", ID: fmt.Sprint(part), Partition: int32(part)}
}
func groupRows(tasks []*execution.Task, offset int64) []execution.TaskBatch {
	batches := make([]execution.TaskBatch, len(tasks))
	for i, task := range tasks {
		batches[i] = execution.TaskBatch{Task: task, Records: []record.Bytes{{Topic: "in", Partition: int32(i), Offset: offset, Timestamp: 10 + offset, Key: []byte("key"), Value: []byte("value")}}}
	}
	return batches
}
func groupTasks(t *testing.T, g *execution.Group, hook func(int, record.Bytes) error, options func(int) []execution.Option) []*execution.Task {
	t.Helper()
	tasks := make([]*execution.Task, 2)
	for i := range tasks {
		var opts []execution.Option
		if options != nil {
			opts = options(i)
		}
		task, err := execution.New(ctx, groupSpec(i), g, opts...)
		if err != nil {
			t.Fatal(err)
		}
		tasks[i] = task
		t.Cleanup(func() {
			if err := task.Close(); err != nil {
				t.Error(err)
			}
		})
		partition := i
		if err := task.Register("in", func(r record.Bytes) error {
			if hook != nil {
				if err := hook(partition, r); err != nil {
					return err
				}
			}
			old, ok, err := task.StoreGet("counts", "key")
			if err != nil {
				return err
			}
			var count int64
			if ok {
				count, err = serde.Int64.Decode(old)
				if err != nil {
					return err
				}
			}
			value, err := serde.Int64.Encode(count + 1)
			if err != nil {
				return err
			}
			task.StorePut("counts", "key", value)
			task.Emit(record.Bytes{Topic: "out", Timestamp: r.Timestamp, Key: r.Key, Value: value})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return tasks
}
func newGroup(t *testing.T, backend execution.AtomicBackend) *execution.Group {
	t.Helper()
	g, err := execution.NewGroup(backend)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestGroupCommitsAllTasksAndReplayOnlyPoll(t *testing.T) {
	backend := &atomicProbe{Backend: memory.New()}
	g := newGroup(t, backend)
	tasks := groupTasks(t, g, nil, nil)
	if err := tasks[0].Process(ctx, groupRows(tasks, 0)[0].Records[0]); !errors.Is(err, execution.ErrGroupRequired) {
		t.Fatal(err)
	}
	if backend.calls != 0 {
		t.Fatal("independent task commit escaped group")
	}
	for offset := int64(0); offset < 2; offset++ {
		if err := g.Process(ctx, groupRows(tasks, offset)); err != nil {
			t.Fatal(err)
		}
	}
	for i, task := range tasks {
		value, ok, err := task.StoreGet("counts", "key")
		if err != nil || !ok {
			t.Fatal(err)
		}
		n, err := serde.Int64.Decode(value)
		if err != nil || n != 2 || len(backend.Outputs(groupSpec(i))) != 2 || task.StreamTime() != 11 {
			t.Fatal("group state/output/checkpoint not published together", n, err)
		}
	}
	if err := g.Process(ctx, groupRows(tasks, 1)); err != nil {
		t.Fatal(err)
	}
	if backend.calls != 3 || backend.empty != 1 {
		t.Fatal("replay-only poll did not reach coordinator", backend.calls, backend.empty)
	}
}

func TestGroupCommitFailuresRestoreAllParticipants(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprintf("ambiguous=%t", ambiguous), func(t *testing.T) {
			backend := &atomicProbe{Backend: memory.New(), failBefore: !ambiguous, failAfter: ambiguous}
			g := newGroup(t, backend)
			tasks := groupTasks(t, g, nil, nil)
			if err := g.Process(ctx, groupRows(tasks, 0)); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			for i, task := range tasks {
				if _, _, err := task.StoreGet("counts", "key"); !errors.Is(err, execution.ErrPoisoned) {
					t.Fatal("participant remained readable", err)
				}
				want := 0
				if ambiguous {
					want = 1
				}
				if len(backend.Outputs(groupSpec(i))) != want {
					t.Fatal("atomic durable outcome differed between tasks")
				}
				if err := task.Close(); err != nil {
					t.Fatal(err)
				}
			}
			backend.failBefore = false
			backend.failAfter = false
			tasks = groupTasks(t, g, nil, nil)
			if err := g.Process(ctx, groupRows(tasks, 0)); err != nil {
				t.Fatal(err)
			}
			for i, task := range tasks {
				value, ok, err := task.StoreGet("counts", "key")
				if err != nil || !ok {
					t.Fatal(err)
				}
				n, err := serde.Int64.Decode(value)
				if err != nil || n != 1 || len(backend.Outputs(groupSpec(i))) != 1 {
					t.Fatal("group recovery lost state or duplicated output", n, err)
				}
			}
		})
	}
}

func TestGroupStaleLastParticipantPreventsEveryDurableWrite(t *testing.T) {
	backend := &atomicProbe{Backend: memory.New()}
	g := newGroup(t, backend)
	tasks := groupTasks(t, g, nil, nil)
	newOwner, err := execution.New(ctx, groupSpec(1), g)
	if err != nil {
		t.Fatal(err)
	}
	defer newOwner.Close()
	if err := g.Process(ctx, groupRows(tasks, 0)); !errors.Is(err, execution.ErrFenced) {
		t.Fatal(err)
	}
	for i, task := range tasks {
		if len(backend.Outputs(groupSpec(i))) != 0 {
			t.Fatal("first participant committed before stale ownership was detected")
		}
		if _, _, err := task.StoreGet("counts", "key"); !errors.Is(err, execution.ErrPoisoned) {
			t.Fatal(err)
		}
	}
}

func TestGroupLocalApplyFailureAfterAtomicCommitPoisonsAll(t *testing.T) {
	backend := &atomicProbe{Backend: memory.New()}
	g := newGroup(t, backend)
	storage := &faultStore{Memory: state.NewMemory()}
	tasks := groupTasks(t, g, nil, func(part int) []execution.Option {
		if part != 1 {
			return nil
		}
		return []execution.Option{execution.WithStoreFactory(func(execution.Spec) (state.Store, error) { return storage, nil })}
	})
	storage.writeFailure = true
	if err := g.Process(ctx, groupRows(tasks, 0)); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	for i, task := range tasks {
		if len(backend.Outputs(groupSpec(i))) != 1 {
			t.Fatal("durable group was not committed before local apply")
		}
		if _, _, err := task.StoreGet("counts", "key"); !errors.Is(err, execution.ErrPoisoned) {
			t.Fatal("partial cache publication remained readable", err)
		}
		if err := task.Close(); err != nil {
			t.Fatal(err)
		}
	}
	tasks = groupTasks(t, g, nil, nil)
	if err := g.Process(ctx, groupRows(tasks, 0)); err != nil {
		t.Fatal(err)
	}
	for i := range tasks {
		if len(backend.Outputs(groupSpec(i))) != 1 {
			t.Fatal("recovery duplicated atomic group output")
		}
	}
}

func TestGroupValidatesAllRoutesBeforeCallbacksAndRejectsForeignTasks(t *testing.T) {
	backend := &atomicProbe{Backend: memory.New()}
	g := newGroup(t, backend)
	calls := 0
	tasks := groupTasks(t, g, func(int, record.Bytes) error { calls++; return nil }, nil)
	invalid := groupRows(tasks, 0)
	invalid[1].Records[0].Topic = "unknown"
	if err := g.Process(ctx, invalid); err == nil || calls != 0 || backend.calls != 0 {
		t.Fatal("partial callback before routing validation", err, calls)
	}
	if err := g.Process(ctx, []execution.TaskBatch{invalid[0], invalid[0]}); err == nil {
		t.Fatal("duplicate group task accepted")
	}
	foreign := newGroup(t, memory.New())
	if err := foreign.Process(ctx, groupRows(tasks, 0)); err == nil {
		t.Fatal("foreign group task accepted")
	}
	if err := g.Process(ctx, groupRows(tasks, 0)); err != nil {
		t.Fatal("routing rejection poisoned healthy task", err)
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

func TestGroupCallbackFailureAndPanicReleaseOwnership(t *testing.T) {
	for _, panics := range []bool{false, true} {
		backend := &atomicProbe{Backend: memory.New()}
		g := newGroup(t, backend)
		tasks := groupTasks(t, g, func(part int, _ record.Bytes) error {
			if part == 1 {
				if panics {
					panic(injected)
				}
				return injected
			}
			return nil
		}, nil)
		if panics {
			var caught any
			func() { defer func() { caught = recover() }(); _ = g.Process(ctx, groupRows(tasks, 0)) }()
			if caught != injected {
				t.Fatal("group changed original panic", caught)
			}
		} else if err := g.Process(ctx, groupRows(tasks, 0)); !errors.Is(err, injected) {
			t.Fatal(err)
		}
		if backend.calls != 0 {
			t.Fatal("failed callbacks reached durable decision")
		}
		for _, task := range tasks {
			if _, _, err := task.StoreGet("counts", "key"); !errors.Is(err, execution.ErrPoisoned) {
				t.Fatal(err)
			}
			if err := task.Close(); err != nil {
				t.Fatal("failed group retained task guard", err)
			}
		}
	}
}

func TestGroupRejectsConcurrentProcessingAndClose(t *testing.T) {
	backend := &atomicProbe{Backend: memory.New()}
	g := newGroup(t, backend)
	entered, release := make(chan struct{}), make(chan struct{})
	tasks := groupTasks(t, g, func(part int, _ record.Bytes) error {
		if part == 0 {
			close(entered)
			<-release
		}
		return nil
	}, nil)
	done := make(chan error, 1)
	go func() { done <- g.Process(ctx, groupRows(tasks, 0)) }()
	<-entered
	if err := g.Process(ctx, groupRows(tasks, 1)); !errors.Is(err, execution.ErrConcurrent) {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if err := task.Close(); !errors.Is(err, execution.ErrConcurrent) {
			t.Fatal(err)
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
