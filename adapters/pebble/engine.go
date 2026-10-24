// Package pebble provides task-owned namespaces on a shared Pebble engine.
package pebble

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/state"
	pebbledb "github.com/cockroachdb/pebble"
)

var ErrInUse = errors.New("persistent state namespace is already owned")

type Config struct {
	Options *pebbledb.Options
	// SyncWrites fsyncs each local write batch. False selects a persistent but
	// rebuildable cache; it does not promise durability across power loss.
	SyncWrites bool
}
type Engine struct {
	mu           sync.Mutex
	db           *pebbledb.DB
	owners       map[string]bool
	closed       bool
	writeOptions *pebbledb.WriteOptions
}
type taskStore struct {
	engine    *Engine
	namespace string
	prefix    []byte
	scratch   []byte
	closed    bool
}

var _ state.Store = (*taskStore)(nil)

func Open(path string, config Config) (*Engine, error) {
	options := config.Options
	if options == nil {
		options = &pebbledb.Options{}
	}
	if config.SyncWrites && options.DisableWAL {
		return nil, errors.New("synchronous durability requires a WAL")
	}
	db, err := pebbledb.Open(path, options)
	if err != nil {
		return nil, err
	}
	writeOptions := pebbledb.NoSync
	if config.SyncWrites {
		writeOptions = pebbledb.Sync
	}
	return &Engine{db: db, owners: make(map[string]bool), writeOptions: writeOptions}, nil
}
func appendField(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}
func prefixEnd(prefix []byte) []byte {
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 255 {
			upper[i]++
			return upper[:i+1]
		}
	}
	panic("state prefix has no upper bound")
}

// Scope grants exclusive logical ownership. Physical engine workers and caches
// are shared across scopes; data access within one scope has a single owner.
func (e *Engine) Scope(namespace string) (state.Store, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, state.ErrClosed
	}
	if namespace == "" {
		return nil, errors.New("state namespace required")
	}
	if e.owners[namespace] {
		return nil, ErrInUse
	}
	e.owners[namespace] = true
	prefix := appendField([]byte{1}, namespace)
	return &taskStore{engine: e, namespace: namespace, prefix: prefix, scratch: make([]byte, 0, 256)}, nil
}

// Task is an execution.StoreFactory keyed by application, task and partition.
func (e *Engine) Task(spec execution.Spec) (state.Store, error) {
	if spec.ApplicationID == "" || spec.ID == "" || spec.Partition < 0 {
		return nil, errors.New("invalid task identity")
	}
	scope := appendField(nil, spec.ApplicationID)
	scope = appendField(scope, spec.ID)
	scope = binary.BigEndian.AppendUint32(scope, uint32(spec.Partition))
	sum := sha256.Sum256(scope)
	return e.Scope(hex.EncodeToString(sum[:]))
}
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	if len(e.owners) > 0 {
		return ErrInUse
	}
	if err := e.db.Close(); err != nil {
		return err
	}
	e.closed = true
	return nil
}
func (s *taskStore) key(store, key string) []byte {
	out := append(s.scratch[:0], s.prefix...)
	out = appendField(out, store)
	out = append(out, key...)
	s.scratch = out
	return out
}
func ownedValue(value []byte) []byte {
	owned := make([]byte, len(value))
	copy(owned, value)
	return owned
}

func (s *taskStore) Get(store, key string) ([]byte, bool, error) {
	if s.closed {
		return nil, false, state.ErrClosed
	}
	value, closer, err := s.engine.db.Get(s.key(store, key))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	owned := ownedValue(value)
	if err = closer.Close(); err != nil {
		return nil, false, err
	}
	return owned, true, nil
}
func (s *taskStore) Scan(store string, visit func(string, []byte)) (err error) {
	if s.closed {
		return state.ErrClosed
	}
	prefix := appendField(append([]byte(nil), s.prefix...), store)
	iterator, err := s.engine.db.NewIter(&pebbledb.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iterator.Close()) }()
	for valid := iterator.First(); valid; valid = iterator.Next() {
		visit(string(iterator.Key()[len(prefix):]), ownedValue(iterator.Value()))
	}
	return iterator.Error()
}
func (s *taskStore) ApplyBatch(mutations []state.Mutation) error {
	if s.closed {
		return state.ErrClosed
	}
	if len(mutations) == 0 {
		return nil
	}
	batch := s.engine.db.NewBatch()
	defer batch.Close()
	for _, mutation := range mutations {
		var err error
		if mutation.Value == nil {
			err = batch.Delete(s.key(mutation.Store, mutation.Key), nil)
		} else {
			err = batch.Set(s.key(mutation.Store, mutation.Key), mutation.Value, nil)
		}
		if err != nil {
			return err
		}
	}
	return batch.Commit(s.engine.writeOptions)
}
func (s *taskStore) Reset() error {
	if s.closed {
		return state.ErrClosed
	}
	batch := s.engine.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(s.prefix, prefixEnd(s.prefix), nil); err != nil {
		return err
	}
	return batch.Commit(s.engine.writeOptions)
}
func (s *taskStore) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.scratch = nil
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	delete(s.engine.owners, s.namespace)
	return nil
}
