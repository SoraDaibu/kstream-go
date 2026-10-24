package execution

import (
	"errors"

	"github.com/SoraDaibu/kstream-go/state"
)

type StoreFactory func(Spec) (state.Store, error)
type taskConfig struct{ store StoreFactory }
type Option func(*taskConfig) error

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
