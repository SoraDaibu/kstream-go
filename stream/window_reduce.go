package stream

import (
	"encoding/binary"
	"errors"

	"github.com/SoraDaibu/kstream-go/internal/graph"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/topology"
	"github.com/SoraDaibu/kstream-go/window"
)

// WindowedReduce emits an update per accepted record in a tumbling window.
// Null input keys/values skip. The first value initializes a window without
// calling reducer. A null reducer result deletes state and emits a tombstone.
func WindowedReduce[K, V any](s KStream[K, V], store string, w window.Tumbling, reducer func(V, V) (V, bool)) KStream[window.Key[K], V] {
	return timeWindowedReduce(s, store, window.Hopping{Size: w.Size, Advance: w.Size, Grace: w.Grace}, reducer)
}

// HoppingWindowedReduce reduces each open overlapping window, in ascending start
// order, with the same null, initialization and deletion rules as WindowedReduce.
func HoppingWindowedReduce[K, V any](s KStream[K, V], store string, w window.Hopping, reducer func(V, V) (V, bool)) KStream[window.Key[K], V] {
	return timeWindowedReduce(s, store, w, reducer)
}

func timeWindowedReduce[K, V any](s KStream[K, V], store string, w window.Hopping, reducer func(V, V) (V, bool)) KStream[window.Key[K], V] {
	s.builder.mutable()
	if !s.partitioned || store == "" || reducer == nil || !s.value.Valid() {
		s.builder.fail(errors.New("windowed reduce requires partitioned keys, store, reducer and valid codec"))
	}
	if err := w.Validate(); err != nil {
		s.builder.fail(err)
	}
	p := &graph.Port[window.Key[K], V]{}
	name := store + "-window-reduce"
	s.builder.add(topology.Node{Name: name, Kind: "window-reduce", Parents: []string{s.name}, Store: store})
	s.port.Subscribe(func(ctx *graph.Context) processor.Handler[K, V] {
		task := ctx.Task
		next := p.Compile(ctx)
		windows := timeWindowState{task: task, store: store, w: w, tracked: make(map[string]struct{})}
		return func(r record.Record[K, V]) error {
			if r.NullKey || r.NullValue {
				return nil
			}
			first, last, clock, err := windows.advance(r.Timestamp)
			if err != nil {
				return err
			}
			if w.Closed(last+w.Size, clock) {
				return nil
			}
			encodedKey, err := s.key.Encode(r.Key)
			if err != nil {
				return err
			}
			if encodedKey == nil {
				return errors.New("non-null key encoded as nil")
			}
			for start := first; ; start += w.Advance {
				end := start + w.Size
				if !w.Closed(end, clock) {
					key := make([]byte, 8+len(encodedKey))
					binary.BigEndian.PutUint64(key, uint64(start))
					copy(key[8:], encodedKey)
					storageKey := string(key)
					stored, present, err := task.StoreGet(store, storageKey)
					if err != nil {
						return err
					}
					result, null, timestamp := r.Value, false, r.Timestamp
					if present {
						old, prior, err := decodeFold(stored, s.value)
						if err != nil {
							return err
						}
						if prior > timestamp {
							timestamp = prior
						}
						result, null = reducer(old, r.Value)
					}
					var encoded []byte
					if !null {
						encoded, err = encodeFold(result, timestamp, s.value)
						if err != nil {
							return err
						}
						if !present {
							windows.track(storageKey, end)
						}
					}
					task.StorePut(store, storageKey, encoded)
					if err := next(record.Record[window.Key[K], V]{Key: window.Key[K]{Key: r.Key, Start: start, End: end}, Value: result, NullValue: null, Timestamp: timestamp}); err != nil {
						return err
					}
				}
				if start == last {
					break
				}
			}
			return nil
		}
	})
	return KStream[window.Key[K], V]{s.builder, p, name, serde.JSON[window.Key[K]](), s.value, true}
}
