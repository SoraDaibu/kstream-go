// Package memory provides an atomic in-process reference backend. It models the
// transaction contract; it is neither durable storage nor a Kafka implementation.
package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/state"
)

type identity struct {
	application, task string
	partition         int32
}
type entry struct {
	generation uint64
	snapshot   execution.Snapshot
	log        []state.Mutation
	outputs    []record.Bytes
}
type Backend struct {
	mu    sync.Mutex
	tasks map[identity]*entry
}

func New() *Backend                 { return &Backend{tasks: make(map[identity]*entry)} }
func key(s execution.Spec) identity { return identity{s.ApplicationID, s.ID, s.Partition} }
func (b *Backend) Open(ctx context.Context, s execution.Spec) (uint64, execution.Snapshot, error) {
	restored := make(state.Image)
	generation, checkpoint, err := b.OpenInto(ctx, s, func(batch []state.Mutation) error {
		for _, mutation := range batch {
			restored.Apply(mutation)
		}
		return nil
	})
	if err != nil {
		return 0, execution.Snapshot{}, err
	}
	return generation, execution.Snapshot{State: restored, NextOffsets: checkpoint.NextOffsets, StreamTime: checkpoint.StreamTime}, nil
}

var _ execution.StreamingRestorer = (*Backend)(nil)

func (b *Backend) OpenInto(ctx context.Context, s execution.Spec, sink execution.RestoreSink) (uint64, execution.Checkpoint, error) {
	if sink == nil {
		return 0, execution.Checkpoint{}, errors.New("restore sink required")
	}
	if err := ctx.Err(); err != nil {
		return 0, execution.Checkpoint{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.tasks[key(s)]
	if e == nil {
		e = &entry{snapshot: execution.Snapshot{State: make(state.Image), NextOffsets: make(map[string]int64), StreamTime: -1}}
		b.tasks[key(s)] = e
	}
	e.generation++
	// Stream committed log order without a second state-sized map. The model
	// still retains its complete log; this is not a durable storage engine.
	for start := 0; start < len(e.log); start += 256 {
		if err := ctx.Err(); err != nil {
			return 0, execution.Checkpoint{}, err
		}
		end := start + 256
		if end > len(e.log) {
			end = len(e.log)
		}
		if err := sink(e.log[start:end:end]); err != nil {
			return 0, execution.Checkpoint{}, err
		}
	}
	offsets := make(map[string]int64)
	for k, v := range e.snapshot.NextOffsets {
		offsets[k] = v
	}
	return e.generation, execution.Checkpoint{NextOffsets: offsets, StreamTime: e.snapshot.StreamTime}, nil
}
func (b *Backend) Commit(ctx context.Context, s execution.Spec, g uint64, tx execution.Transaction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.tasks[key(s)]
	if e == nil || e.generation != g {
		return execution.ErrFenced
	}
	apply(e, tx)
	return nil
}

// CommitTasks is an executable model of one atomic multi-task decision. Validate
// all generations under the infrastructure lock before publishing any changes.
func (b *Backend) CommitTasks(ctx context.Context, transactions []execution.TaskTransaction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	seen := make(map[identity]bool, len(transactions))
	for _, tx := range transactions {
		id := key(tx.Spec)
		if seen[id] {
			return errors.New("duplicate transaction participant")
		}
		seen[id] = true
		e := b.tasks[id]
		if e == nil || e.generation != tx.Generation {
			return execution.ErrFenced
		}
	}
	for _, tx := range transactions {
		apply(b.tasks[key(tx.Spec)], tx.Transaction)
	}
	return nil
}

var _ execution.AtomicBackend = (*Backend)(nil)

func apply(e *entry, tx execution.Transaction) {
	for _, m := range tx.Mutations {
		m.Value = record.CloneBytes(m.Value)
		e.log = append(e.log, m)
	}
	for _, r := range tx.Outputs {
		e.outputs = append(e.outputs, r.Clone())
	}
	for topic, offset := range tx.NextOffsets {
		e.snapshot.NextOffsets[topic] = offset
	}
	e.snapshot.StreamTime = tx.StreamTime
}
func (b *Backend) Outputs(s execution.Spec) []record.Bytes {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.tasks[key(s)]
	if e == nil {
		return nil
	}
	out := make([]record.Bytes, len(e.outputs))
	for i, r := range e.outputs {
		out[i] = r.Clone()
	}
	return out
}
