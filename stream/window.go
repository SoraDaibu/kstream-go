package stream

import (
	"encoding/binary"
	"errors"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/topology"
	"github.com/SoraDaibu/kstream-go/window"
)

// WindowedCount emits an update per accepted record; closed windows are evicted.
// This baseline scans windows when stream-time advances. A persistent store
// needs an expiry index and write batches for large-window workloads.
func WindowedCount[K, V any](s KStream[K, V], store string, w window.Tumbling) KStream[window.Key[K], int64] {
	s.builder.mutable()
	if !s.partitioned {
		s.builder.fail(errors.New("windowed count requires repartition"))
	}
	if err := w.Validate(); err != nil {
		s.builder.fail(err)
	}
	if store == "" {
		s.builder.fail(errors.New("window store required"))
	}
	p := &port[window.Key[K], int64]{}
	name := store + "-window-count"
	s.builder.add(topology.Node{Name: name, Kind: "window-count", Parents: []string{s.name}, Store: store})
	s.port.subscribers = append(s.port.subscribers, func(t *execution.Task) processor.Handler[K, V] {
		next := p.compile(t)
		return func(r record.Record[K, V]) error {
			if r.NullKey {
				return nil
			}
			start, end, err := w.Bounds(r.Timestamp)
			if err != nil {
				return err
			}
			// The empty store key holds operator-local stream-time. Window keys
			// always contain at least the eight-byte window-start prefix.
			clock := r.Timestamp
			advanced := true
			if raw, ok := t.StoreGet(store, ""); ok {
				previous, err := serde.Int64.Decode(raw)
				if err != nil {
					return err
				}
				if previous >= clock {
					clock = previous
					advanced = false
				}
			}
			if advanced {
				encoded, err := serde.Int64.Encode(clock)
				if err != nil {
					return err
				}
				t.StorePut(store, "", encoded)
				t.StoreScan(store, func(key string, _ []byte) {
					if len(key) < 8 {
						return
					}
					start := int64(binary.BigEndian.Uint64([]byte(key[:8])))
					if w.Closed(start+w.Size, clock) {
						t.StorePut(store, key, nil)
					}
				})
			}
			if w.Closed(end, clock) {
				return nil
			}
			encoded, err := s.key.Encode(r.Key)
			if err != nil {
				return err
			}
			if encoded == nil {
				return errors.New("non-null key encoded as nil")
			}
			key := make([]byte, 8+len(encoded))
			binary.BigEndian.PutUint64(key, uint64(start))
			copy(key[8:], encoded)
			count, timestamp := int64(0), r.Timestamp
			if old, ok := t.StoreGet(store, string(key)); ok {
				var prior int64
				count, prior, err = decodeAggregate(old)
				if err != nil {
					return err
				}
				if prior > timestamp {
					timestamp = prior
				}
			}
			count++
			t.StorePut(store, string(key), encodeAggregate(count, timestamp))
			return next(record.Record[window.Key[K], int64]{
				Key:   window.Key[K]{Key: r.Key, Start: start, End: end},
				Value: count, Timestamp: timestamp,
			})
		}
	})
	return KStream[window.Key[K], int64]{s.builder, p, name, serde.JSON[window.Key[K]](), serde.Int64, true}
}
