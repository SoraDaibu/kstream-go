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

func timeWindowedFold[K, V, A any](s KStream[K, V], store string, w window.Hopping, kind string, value serde.Codec[A], apply func(K, V, bool, A, bool) (A, bool)) KStream[window.Key[K], A] {
	s.builder.mutable()
	if !s.partitioned || store == "" || apply == nil || !value.Valid() {
		s.builder.fail(errors.New("windowed aggregation requires partitioned keys, store, apply function and valid codec"))
	}
	if err := w.Validate(); err != nil {
		s.builder.fail(err)
	}
	p := &graph.Port[window.Key[K], A]{}
	name := store + "-" + kind
	s.builder.add(topology.Node{Name: name, Kind: kind, Parents: []string{s.name}, Store: store})
	s.port.Subscribe(func(ctx *graph.Context) processor.Handler[K, V] {
		task := ctx.Task
		next := p.Compile(ctx)
		windows := timeWindowState{task: task, store: store, w: w, tracked: make(map[string]struct{})}
		return func(r record.Record[K, V]) error {
			if r.NullKey {
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
					var old A
					timestamp := r.Timestamp
					if present {
						var prior int64
						old, prior, err = decodeFold(stored, value)
						if err != nil {
							return err
						}
						if prior > timestamp {
							timestamp = prior
						}
					}
					result, null := apply(r.Key, r.Value, r.NullValue, old, present)
					var encoded []byte
					if !null {
						encoded, err = encodeFold(result, timestamp, value)
						if err != nil {
							return err
						}
						if !present {
							windows.track(storageKey, end)
						}
					}
					task.StorePut(store, storageKey, encoded)
					if err := next(record.Record[window.Key[K], A]{Key: window.Key[K]{Key: r.Key, Start: start, End: end}, Value: result, NullValue: null, Timestamp: timestamp}); err != nil {
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
	return KStream[window.Key[K], A]{s.builder, p, name, serde.JSON[window.Key[K]](), value, true}
}
