// Package memory provides an atomic in-process reference backend. It models the
// transaction contract; it is neither durable storage nor a Kafka implementation.
package memory

import (
	"context"
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
	if err := ctx.Err(); err != nil {
		return 0, execution.Snapshot{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.tasks[key(s)]
	if e == nil {
		e = &entry{snapshot: execution.Snapshot{State: make(state.Image), NextOffsets: make(map[string]int64), StreamTime: -1}}
		b.tasks[key(s)] = e
	}
	e.generation++
	// Rebuild from committed changelog, rather than trusting a task's local cache.
	restored := make(state.Image)
	for _, m := range e.log {
		restored.Apply(m)
	}
	offsets := make(map[string]int64)
	for k, v := range e.snapshot.NextOffsets {
		offsets[k] = v
	}
	return e.generation, execution.Snapshot{State: restored, NextOffsets: offsets, StreamTime: e.snapshot.StreamTime}, nil
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
	return nil
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
