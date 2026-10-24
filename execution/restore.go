package execution

import (
	"context"

	"github.com/SoraDaibu/kstream-go/state"
)

// Checkpoint is the committed input position and task clock after restoration.
type Checkpoint struct {
	NextOffsets map[string]int64
	StreamTime  int64
}

// RestoreSink consumes an ordered bounded batch synchronously. Mutation values
// are borrowed and read-only: copy before returning if retaining any buffers. Returning
// an error aborts initialization; no partially restored task may be published.
type RestoreSink func([]state.Mutation) error

// StreamingRestorer is an optional backend capability selected only at setup.
// OpenInto fences old ownership and restores a committed prefix into the sink.
// State remains inaccessible until this method succeeds. Never invoke backend
// methods recursively from the sink. Open remains the legacy snapshot interface.
type StreamingRestorer interface {
	Backend
	OpenInto(context.Context, Spec, RestoreSink) (uint64, Checkpoint, error)
}

func restoreBackend(ctx context.Context, spec Spec, backend Backend, sink RestoreSink) (uint64, Checkpoint, error) {
	if restorer, ok := backend.(StreamingRestorer); ok {
		return restorer.OpenInto(ctx, spec, sink)
	}
	generation, snapshot, err := backend.Open(ctx, spec)
	if err != nil {
		return 0, Checkpoint{}, err
	}
	// Legacy backends materialize an image, but additional mutation scratch
	// space remains bounded while loading it into a task store.
	mutations := make([]state.Mutation, 0, 256)
	for store, entries := range snapshot.State {
		for key, value := range entries {
			mutations = append(mutations, state.Mutation{Store: store, Key: key, Value: value})
			if len(mutations) == cap(mutations) {
				if err := sink(mutations); err != nil {
					return 0, Checkpoint{}, err
				}
				clear(mutations)
				mutations = mutations[:0]
			}
		}
	}
	if len(mutations) > 0 {
		if err := sink(mutations); err != nil {
			return 0, Checkpoint{}, err
		}
	}
	return generation, Checkpoint{NextOffsets: snapshot.NextOffsets, StreamTime: snapshot.StreamTime}, nil
}
