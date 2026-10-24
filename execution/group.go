package execution

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/SoraDaibu/kstream-go/record"
)

var ErrGroupRequired = errors.New("task belongs to a transaction group; use Group.Process")

// TaskTransaction is one participant in an atomic durable decision. Its buffers
// are read-only and valid until CommitTasks returns; copy before retaining them.
type TaskTransaction struct {
	Spec        Spec
	Generation  uint64
	Transaction Transaction
}

// AtomicBackend validates every participant's generation before atomically
// publishing any state, output or checkpoint. Errors may be ambiguous. An empty
// transaction set may still commit an adapter's polled consumer-group offsets.
type AtomicBackend interface {
	Backend
	CommitTasks(context.Context, []TaskTransaction) error
}

// Group owns one worker's multi-task durable boundary. Setup, polling and
// processing have one owner. Task stores remain independently owned, and a
// processing call runs their callbacks synchronously without new goroutines.
type Group struct {
	backend AtomicBackend
	active  atomic.Bool
}

func NewGroup(backend AtomicBackend) (*Group, error) {
	if backend == nil {
		return nil, errors.New("atomic backend required")
	}
	return &Group{backend: backend}, nil
}
func (g *Group) Open(ctx context.Context, spec Spec) (uint64, Snapshot, error) {
	if g == nil || g.backend == nil {
		return 0, Snapshot{}, errors.New("uninitialized transaction group")
	}
	return g.backend.Open(ctx, spec)
}
func (g *Group) OpenInto(ctx context.Context, spec Spec, sink RestoreSink) (uint64, Checkpoint, error) {
	if g == nil || g.backend == nil {
		return 0, Checkpoint{}, errors.New("uninitialized transaction group")
	}
	if sink == nil {
		return 0, Checkpoint{}, errors.New("restore sink required")
	}
	return restoreBackend(ctx, spec, g.backend, sink)
}

// Commit rejects an accidental independent commit from a group-owned task.
func (*Group) Commit(context.Context, Spec, uint64, Transaction) error { return ErrGroupRequired }

// TaskBatch is a caller-bounded ordered slice for one task. A task may appear
// only once per group call. Create topology tasks with the Group as backend.
type TaskBatch struct {
	Task    *Task
	Records []record.Bytes
}

// Process stages all tasks before making one atomic backend decision. It applies
// every local batch before publishing any checkpoint. Processing/commit/apply
// errors poison all participants; restore fresh tasks after an error. Routing
// validation or busy-owner errors before callbacks leave healthy tasks reusable.
func (g *Group) Process(ctx context.Context, batches []TaskBatch) (err error) {
	if g == nil || g.backend == nil {
		return errors.New("uninitialized transaction group")
	}
	if !g.active.CompareAndSwap(false, true) {
		return ErrConcurrent
	}
	defer g.active.Store(false)
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(batches) == 0 {
		return nil
	}
	seen := make(map[*Task]bool, len(batches))
	// Check membership and duplicates before acquiring any task guard.
	for _, batch := range batches {
		if batch.Task == nil || seen[batch.Task] {
			return errors.New("nil or duplicate transaction group task")
		}
		if batch.Task.group != g {
			return errors.New("task belongs to a different transaction group")
		}
		seen[batch.Task] = true
	}
	acquired := 0
	started := false
	completed := false
	defer func() {
		panicValue := recover()
		for _, batch := range batches[:acquired] {
			batch.Task.clearPending()
			if started && !completed {
				batch.Task.poisoned = true
			}
			batch.Task.active.Store(false)
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()
	for _, batch := range batches {
		if !batch.Task.active.CompareAndSwap(false, true) {
			return ErrConcurrent
		}
		acquired++
	}
	for _, batch := range batches {
		if err := batch.Task.validate(ctx, batch.Records); err != nil {
			return err
		}
	}
	started = true
	transactions := make([]TaskTransaction, 0, len(batches))
	for _, batch := range batches {
		tx, stageErr := batch.Task.stage(ctx, batch.Records)
		if stageErr != nil {
			return fmt.Errorf("stage task %s: %w", batch.Task.spec.ID, stageErr)
		}
		if len(tx.NextOffsets) > 0 {
			transactions = append(transactions, TaskTransaction{Spec: batch.Task.spec, Generation: batch.Task.generation, Transaction: *tx})
		}
	}
	if err = g.backend.CommitTasks(ctx, transactions); err != nil {
		return err
	}
	for _, batch := range batches {
		if len(batch.Task.pending.NextOffsets) == 0 {
			continue
		}
		if err = batch.Task.apply(batch.Task.pending); err != nil {
			return err
		}
	}
	for _, batch := range batches {
		batch.Task.publish(batch.Task.pending)
	}
	completed = true
	return nil
}
