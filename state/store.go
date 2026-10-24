package state

import (
	"errors"

	"github.com/SoraDaibu/kstream-go/record"
)

var ErrClosed = errors.New("state store closed")

// Store belongs to one task. Get and Scan return owned value bytes. ApplyBatch
// publishes all mutations atomically; its durability depends on the adapter's
// explicit configuration. A task must restore from its backend after an error.
// Reset is used only during exclusive task initialization.
type Store interface {
	Get(store, key string) ([]byte, bool, error)
	Scan(store string, visit func(string, []byte)) error
	ApplyBatch([]Mutation) error
	Reset() error
	Close() error
}

type Memory struct {
	image  Image
	closed bool
}

func NewMemory() *Memory { return &Memory{image: make(Image)} }
func (m *Memory) Get(store, key string) ([]byte, bool, error) {
	if m.closed {
		return nil, false, ErrClosed
	}
	value, ok := m.image[store][key]
	return record.CloneBytes(value), ok, nil
}
func (m *Memory) Scan(store string, visit func(string, []byte)) error {
	if m.closed {
		return ErrClosed
	}
	for key, value := range m.image[store] {
		visit(key, record.CloneBytes(value))
	}
	return nil
}
func (m *Memory) ApplyBatch(mutations []Mutation) error {
	if m.closed {
		return ErrClosed
	}
	for _, mutation := range mutations {
		m.image.Apply(mutation)
	}
	return nil
}
func (m *Memory) Reset() error {
	if m.closed {
		return ErrClosed
	}
	m.image = make(Image)
	return nil
}
func (m *Memory) Close() error { m.closed = true; m.image = nil; return nil }
