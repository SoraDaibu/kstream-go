// Package execution coordinates serial partition tasks and transaction backends.
package execution

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/state"
)

var ErrFenced = errors.New("task generation fenced")
var ErrPoisoned = errors.New("task failed; restore a new incarnation")
var ErrConcurrent = errors.New("concurrent task access")

type Spec struct {
	ApplicationID, ID string
	Partition         int32
}
type Snapshot struct {
	State       state.Image
	NextOffsets map[string]int64
	StreamTime  int64
}
type Transaction struct {
	Mutations   []state.Mutation
	Outputs     []record.Bytes
	NextOffsets map[string]int64
	StreamTime  int64
}

// Backend must atomically persist mutations, outputs, next offsets and stream-time.
// Open fences older generations and returns a consistent committed snapshot.
// A Commit error may be ambiguous; callers must restore, never retry in place.
type Backend interface {
	Open(context.Context, Spec) (uint64, Snapshot, error)
	Commit(context.Context, Spec, uint64, Transaction) error
}
type Task struct {
	spec       Spec
	backend    Backend
	generation uint64
	image      state.Image
	offsets    map[string]int64
	streamTime int64
	handlers   map[string]func(record.Bytes) error
	pending    *Transaction
	active     atomic.Bool
	poisoned   bool
}

func New(ctx context.Context, s Spec, b Backend) (*Task, error) {
	if b == nil || s.ApplicationID == "" || s.ID == "" || s.Partition < 0 {
		return nil, errors.New("application, task ID, nonnegative partition and backend required")
	}
	g, snapshot, err := b.Open(ctx, s)
	if err != nil {
		return nil, err
	}
	if snapshot.State == nil {
		snapshot.State = make(state.Image)
	}
	if snapshot.NextOffsets == nil {
		snapshot.NextOffsets = make(map[string]int64)
	}
	return &Task{spec: s, backend: b, generation: g, image: snapshot.State.Clone(), offsets: cloneOffsets(snapshot.NextOffsets), streamTime: snapshot.StreamTime, handlers: make(map[string]func(record.Bytes) error)}, nil
}
func cloneOffsets(m map[string]int64) map[string]int64 {
	c := make(map[string]int64, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// Register is setup-only and must run before the task is handed to a scheduler.
func (t *Task) Register(topic string, h func(record.Bytes) error) error {
	if topic == "" || h == nil || t.handlers[topic] != nil {
		return fmt.Errorf("invalid or duplicate source %q", topic)
	}
	t.handlers[topic] = h
	return nil
}
func (t *Task) Process(ctx context.Context, r record.Bytes) (err error) {
	if !t.active.CompareAndSwap(false, true) {
		return ErrConcurrent
	}
	defer t.active.Store(false)
	if t.poisoned {
		return ErrPoisoned
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	h := t.handlers[r.Topic]
	if h == nil {
		return fmt.Errorf("unknown source %q", r.Topic)
	}
	if r.Partition != t.spec.Partition {
		return errors.New("input partition does not belong to task")
	}
	if r.Offset < 0 || r.Offset == math.MaxInt64 || r.Timestamp < 0 {
		return errors.New("invalid offset or negative timestamp")
	}
	if r.Offset < t.offsets[r.Topic] {
		return nil
	}
	streamTime := t.streamTime
	if r.Timestamp > streamTime {
		streamTime = r.Timestamp
	}
	tx := Transaction{NextOffsets: map[string]int64{r.Topic: r.Offset + 1}, StreamTime: streamTime}
	t.pending = &tx
	defer func() {
		t.pending = nil
		if p := recover(); p != nil {
			t.poisoned = true
			panic(p)
		}
		if err != nil {
			t.poisoned = true
		}
	}()
	if err = h(r); err != nil {
		return err
	}
	if err = t.backend.Commit(ctx, t.spec, t.generation, tx); err != nil {
		return err
	}
	for _, m := range tx.Mutations {
		t.image.Apply(m)
	}
	t.offsets[r.Topic] = r.Offset + 1
	t.streamTime = streamTime
	return nil
}
func (t *Task) requirePending() {
	if t.pending == nil {
		panic("processor context used outside a transaction")
	}
}
func (t *Task) StreamTime() int64 {
	if t.pending != nil {
		return t.pending.StreamTime
	}
	return t.streamTime
}
func (t *Task) StoreGet(store, key string) ([]byte, bool) {
	if t.pending != nil {
		for i := len(t.pending.Mutations) - 1; i >= 0; i-- {
			m := t.pending.Mutations[i]
			if m.Store == store && m.Key == key {
				return record.CloneBytes(m.Value), m.Value != nil
			}
		}
	}
	v, ok := t.image[store][key]
	return record.CloneBytes(v), ok
}
func (t *Task) StorePut(store, key string, value []byte) {
	t.requirePending()
	t.pending.Mutations = append(t.pending.Mutations, state.Mutation{Store: store, Key: key, Value: record.CloneBytes(value)})
}
func (t *Task) StoreScan(store string, visit func(string, []byte)) {
	entries := make(map[string][]byte)
	for k, v := range t.image[store] {
		entries[k] = v
	}
	if t.pending != nil {
		for _, m := range t.pending.Mutations {
			if m.Store == store {
				if m.Value == nil {
					delete(entries, m.Key)
				} else {
					entries[m.Key] = m.Value
				}
			}
		}
	}
	for k, v := range entries {
		visit(k, record.CloneBytes(v))
	}
}
func (t *Task) Emit(r record.Bytes) {
	t.requirePending()
	r.Partition = t.spec.Partition
	r.Offset = -1
	t.pending.Outputs = append(t.pending.Outputs, r.Clone())
}
