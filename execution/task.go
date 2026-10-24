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
var ErrClosed = errors.New("task closed")

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
type stateLocation struct{ store, key string }

type Task struct {
	spec              Spec
	backend           Backend
	group             *Group
	generation        uint64
	store             state.Store
	offsets           map[string]int64
	streamTime        int64
	handlers          map[string]func(record.Bytes) error
	pending           *Transaction
	pendingIndex      map[stateLocation]int
	active            atomic.Bool
	poisoned          bool
	closed            bool
	pendingStoreError error
}

func New(ctx context.Context, s Spec, b Backend, options ...Option) (*Task, error) {
	if b == nil || s.ApplicationID == "" || s.ID == "" || s.Partition < 0 {
		return nil, errors.New("application, task ID, nonnegative partition and backend required")
	}
	config := taskConfig{store: func(Spec) (state.Store, error) { return state.NewMemory(), nil }}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("nil task option")
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	storage, err := config.store(s)
	if err != nil {
		return nil, err
	}
	if storage == nil {
		return nil, errors.New("store factory returned nil")
	}
	initialized := false
	defer func() {
		if !initialized {
			_ = storage.Close()
		}
	}()
	if err = storage.Reset(); err != nil {
		return nil, err
	}
	generation, checkpoint, err := restoreBackend(ctx, s, b, storage.ApplyBatch)
	if err != nil {
		return nil, err
	}
	task := &Task{spec: s, backend: b, generation: generation, store: storage, offsets: cloneOffsets(checkpoint.NextOffsets), streamTime: checkpoint.StreamTime, handlers: make(map[string]func(record.Bytes) error)}
	task.group, _ = b.(*Group)
	initialized = true
	return task, nil
}

// Close releases task-local state. Stop processing first; an active task cannot
// be closed concurrently. The caller retains ownership of the shared backend.
func (t *Task) Close() error {
	if !t.active.CompareAndSwap(false, true) {
		return ErrConcurrent
	}
	defer t.active.Store(false)
	if t.closed {
		return nil
	}
	t.closed = true
	return t.store.Close()
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

// Process commits one input record. Use ProcessBatch to amortize transaction cost.
func (t *Task) Process(ctx context.Context, r record.Bytes) error {
	return t.ProcessBatch(ctx, []record.Bytes{r})
}

// ProcessBatch processes an ordered, caller-bounded slice atomically. Every
// accepted record observes preceding staged state and the clock as of that
// record. No output, store mutation or input offset is published before commit.
// Already committed offsets and repeated offsets in this batch are skipped.
// Callback or commit failure poisons the task; restore a fresh incarnation.
func (t *Task) ProcessBatch(ctx context.Context, records []record.Bytes) (err error) {
	if !t.active.CompareAndSwap(false, true) {
		return ErrConcurrent
	}
	defer t.active.Store(false)
	if err = t.validate(ctx, records); err != nil {
		return err
	}
	if t.group != nil {
		return ErrGroupRequired
	}
	defer t.finish(&err)
	tx, err := t.stage(ctx, records)
	if err != nil || len(tx.NextOffsets) == 0 {
		return err
	}
	if err = t.backend.Commit(ctx, t.spec, t.generation, *tx); err != nil {
		return err
	}
	if err = t.apply(tx); err != nil {
		return err
	}
	t.publish(tx)
	return nil
}
func (t *Task) validate(ctx context.Context, records []record.Bytes) error {
	if t.closed {
		return ErrClosed
	}
	if t.poisoned {
		return ErrPoisoned
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Validate all routing/metadata before invoking callbacks. Validation errors
	// therefore do not poison an otherwise healthy task.
	for _, r := range records {
		if t.handlers[r.Topic] == nil {
			return fmt.Errorf("unknown source %q", r.Topic)
		}
		if r.Partition != t.spec.Partition {
			return errors.New("input partition does not belong to task")
		}
		if r.Offset < 0 || r.Offset == math.MaxInt64 || r.Timestamp < 0 {
			return errors.New("invalid offset or negative timestamp")
		}
	}
	return nil
}

func (t *Task) clearPending() {
	t.pending = nil
	t.pendingIndex = nil
	t.pendingStoreError = nil
}
func (t *Task) finish(err *error) {
	t.clearPending()
	if p := recover(); p != nil {
		t.poisoned = true
		panic(p)
	}
	if *err != nil {
		t.poisoned = true
	}
}

func (t *Task) stage(ctx context.Context, records []record.Bytes) (*Transaction, error) {
	tx := Transaction{StreamTime: t.streamTime}
	t.pending = &tx
	for _, r := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		next, ok := tx.NextOffsets[r.Topic]
		if !ok {
			next = t.offsets[r.Topic]
		}
		if r.Offset < next {
			continue
		}
		if tx.NextOffsets == nil {
			tx.NextOffsets = make(map[string]int64)
		}
		if r.Timestamp > tx.StreamTime {
			tx.StreamTime = r.Timestamp
		}
		if err := t.handlers[r.Topic](r); err != nil {
			return nil, err
		}
		if t.pendingStoreError != nil {
			return nil, t.pendingStoreError
		}
		tx.NextOffsets[r.Topic] = r.Offset + 1
	}
	return &tx, nil
}
func (t *Task) apply(tx *Transaction) error {
	if err := t.store.ApplyBatch(tx.Mutations); err != nil {
		return fmt.Errorf("backend committed but local state apply failed; restore task: %w", err)
	}
	return nil
}
func (t *Task) publish(tx *Transaction) {
	for topic, offset := range tx.NextOffsets {
		t.offsets[topic] = offset
	}
	t.streamTime = tx.StreamTime
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

// mutationIndex uses a short linear scan for small write sets and promotes to
// a map for larger batches. Single-key aggregates need no per-transaction map.
func (t *Task) mutationIndex(store, key string) (int, bool) {
	if t.pendingIndex != nil {
		i, ok := t.pendingIndex[stateLocation{store, key}]
		return i, ok
	}
	for i := len(t.pending.Mutations) - 1; i >= 0; i-- {
		m := t.pending.Mutations[i]
		if m.Store == store && m.Key == key {
			return i, true
		}
	}
	return 0, false
}
func (t *Task) storeError(err error) error {
	if err != nil && t.pending != nil && t.pendingStoreError == nil {
		t.pendingStoreError = err
	}
	return err
}
func (t *Task) StoreGet(store, key string) ([]byte, bool, error) {
	if t.closed {
		return nil, false, ErrClosed
	}
	if t.poisoned {
		return nil, false, ErrPoisoned
	}
	if t.pendingStoreError != nil {
		return nil, false, t.pendingStoreError
	}
	if t.pending != nil {
		if i, ok := t.mutationIndex(store, key); ok {
			value := t.pending.Mutations[i].Value
			return record.CloneBytes(value), value != nil, nil
		}
	}
	value, ok, err := t.store.Get(store, key)
	return value, ok, t.storeError(err)
}
func (t *Task) StorePut(store, key string, value []byte) {
	t.requirePending()
	if i, ok := t.mutationIndex(store, key); ok {
		t.pending.Mutations[i].Value = record.CloneBytes(value)
		return
	}
	if t.pendingIndex == nil && len(t.pending.Mutations) >= 8 {
		t.pendingIndex = make(map[stateLocation]int, 16)
		for i, m := range t.pending.Mutations {
			t.pendingIndex[stateLocation{m.Store, m.Key}] = i
		}
	}
	if t.pendingIndex != nil {
		t.pendingIndex[stateLocation{store, key}] = len(t.pending.Mutations)
	}
	t.pending.Mutations = append(t.pending.Mutations, state.Mutation{Store: store, Key: key, Value: record.CloneBytes(value)})
}
func (t *Task) StoreScan(store string, visit func(string, []byte)) error {
	if t.closed {
		return ErrClosed
	}
	if t.poisoned {
		return ErrPoisoned
	}
	if t.pendingStoreError != nil {
		return t.pendingStoreError
	}
	entries := make(map[string][]byte)
	if err := t.store.Scan(store, func(key string, value []byte) { entries[key] = value }); err != nil {
		return t.storeError(err)
	}
	if t.pending != nil {
		for _, mutation := range t.pending.Mutations {
			if mutation.Store == store {
				if mutation.Value == nil {
					delete(entries, mutation.Key)
				} else {
					entries[mutation.Key] = record.CloneBytes(mutation.Value)
				}
			}
		}
	}
	for key, value := range entries {
		visit(key, value)
	}
	return nil
}
func (t *Task) Emit(r record.Bytes) {
	t.requirePending()
	r.Partition = t.spec.Partition
	r.Offset = -1
	t.pending.Outputs = append(t.pending.Outputs, r.Clone())
}
