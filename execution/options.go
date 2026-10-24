package execution

import (
	"errors"

	"github.com/SoraDaibu/kstream-go/state"
)

type StoreFactory func(Spec) (state.Store, error)
type taskConfig struct {
	store          StoreFactory
	recordMetadata bool
	outputCapacity int
}
type Option func(*taskConfig) error

// WithRecordMetadata enables current-input metadata for lower-level registered
// handlers. The DSL enables this automatically for custom Process factories.
func WithRecordMetadata() Option {
	return func(config *taskConfig) error { config.recordMetadata = true; return nil }
}

// WithOutputCapacity reserves up to capacity record headers on the first Emit
// of each transaction. Zero keeps append growth. It is not an output limit;
// choose it for expected emission rates, since sparse output can waste memory.
func WithOutputCapacity(capacity int) Option {
	return func(config *taskConfig) error {
		if capacity < 0 || capacity > 65536 {
			return errors.New("output capacity must be between 0 and 65536")
		}
		config.outputCapacity = capacity
		return nil
	}
}

// WithStoreFactory transfers ownership of a fresh task store to the runtime.
// The runtime closes it on initialization failure or Task.Close. A factory must
// not return a logical store already owned by another task.
func WithStoreFactory(factory StoreFactory) Option {
	return func(config *taskConfig) error {
		if factory == nil {
			return errors.New("nil store factory")
		}
		config.store = factory
		return nil
	}
}
