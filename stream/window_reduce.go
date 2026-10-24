package stream

import (
	"errors"
	"github.com/SoraDaibu/kstream-go/window"
)

// WindowedReduce emits an update per accepted record in a tumbling window.
// Null input keys skip. A first null value emits a tombstone. The first value initializes a window without
// calling reducer. The callback receives valueNull for subsequent inputs. A null
// reducer result deletes state and emits a tombstone.
func WindowedReduce[K, V any](s KStream[K, V], store string, w window.Tumbling, reducer func(previous V, value V, valueNull bool) (V, bool)) KStream[window.Key[K], V] {
	return timeWindowedReduce(s, store, window.Hopping{Size: w.Size, Advance: w.Size, Grace: w.Grace}, reducer)
}

// HoppingWindowedReduce reduces each open overlapping window, in ascending start
// order, with the same null, initialization and deletion rules as WindowedReduce.
func HoppingWindowedReduce[K, V any](s KStream[K, V], store string, w window.Hopping, reducer func(previous V, value V, valueNull bool) (V, bool)) KStream[window.Key[K], V] {
	return timeWindowedReduce(s, store, w, reducer)
}

func timeWindowedReduce[K, V any](s KStream[K, V], store string, w window.Hopping, reducer func(previous V, value V, valueNull bool) (V, bool)) KStream[window.Key[K], V] {
	s.builder.mutable()
	if reducer == nil {
		s.builder.fail(errors.New("windowed reduce requires a reducer"))
	}
	return timeWindowedFold(s, store, w, "window-reduce", s.value,
		func(_ K, input V, inputNull bool, old V, present bool) (V, bool) {
			if !present {
				return input, inputNull
			}
			return reducer(old, input, inputNull)
		})
}
