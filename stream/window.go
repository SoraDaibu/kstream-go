package stream

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/SoraDaibu/kstream-go/internal/graph"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/topology"
	"github.com/SoraDaibu/kstream-go/window"
)

// WindowedCount emits an update per accepted record; closed windows are evicted.
// A task-local expiry index is rebuilt from committed windows once per task.
func WindowedCount[K, V any](s KStream[K, V], store string, w window.Tumbling) KStream[window.Key[K], int64] {
	return timeWindowedCount(s, store, window.Hopping{Size: w.Size, Advance: w.Size, Grace: w.Grace})
}

// HoppingWindowedCount emits an update for every open window containing each
// record, in ascending window-start order. Null keys skip; null values count.
func HoppingWindowedCount[K, V any](s KStream[K, V], store string, w window.Hopping) KStream[window.Key[K], int64] {
	return timeWindowedCount(s, store, w)
}

func timeWindowedCount[K, V any](s KStream[K, V], store string, w window.Hopping) KStream[window.Key[K], int64] {
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
	p := &graph.Port[window.Key[K], int64]{}
	name := store + "-window-count"
	s.builder.add(topology.Node{Name: name, Kind: "window-count", Parents: []string{s.name}, Store: store})
	s.port.Subscribe(func(t *graph.Context) processor.Handler[K, V] {
		task := t.Task
		next := p.Compile(t)
		var expiry windowExpiryHeap
		initialized := false
		return func(r record.Record[K, V]) error {
			if r.NullKey {
				return nil
			}
			first, last, err := w.Starts(r.Timestamp)
			if err != nil {
				return err
			}
			if !initialized {
				var invalid error
				if err := task.StoreScan(store, func(key string, _ []byte) {
					if key == "" {
						return // Operator-local clock metadata.
					}
					if len(key) < 8 {
						invalid = errors.New("invalid stored window key")
						return
					}
					start := int64(binary.BigEndian.Uint64([]byte(key[:8])))
					if start < 0 || start > math.MaxInt64-w.Size {
						invalid = errors.New("invalid stored window bounds")
						return
					}
					expiry = append(expiry, windowExpiry{end: start + w.Size, key: key})
				}); err != nil {
					return err
				}
				if invalid != nil {
					return invalid
				}
				expiry.initialize()
				initialized = true
			}
			// The empty store key holds operator-local stream-time. Window keys
			// always contain at least the eight-byte window-start prefix.
			clock := r.Timestamp
			advanced := true
			raw, ok, err := task.StoreGet(store, "")
			if err != nil {
				return err
			}
			if ok {
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
				task.StorePut(store, "", encoded)
				for len(expiry) > 0 && w.Closed(expiry[0].end, clock) {
					task.StorePut(store, expiry.pop().key, nil)
				}
			}
			if w.Closed(last+w.Size, clock) {
				return nil // All earlier overlapping windows are also closed.
			}
			encoded, err := s.key.Encode(r.Key)
			if err != nil {
				return err
			}
			if encoded == nil {
				return errors.New("non-null key encoded as nil")
			}
			for start := first; ; start += w.Advance {
				end := start + w.Size
				if !w.Closed(end, clock) {
					key := make([]byte, 8+len(encoded))
					binary.BigEndian.PutUint64(key, uint64(start))
					copy(key[8:], encoded)
					count, timestamp := int64(0), r.Timestamp
					storageKey := string(key)
					old, ok, err := task.StoreGet(store, storageKey)
					if err != nil {
						return err
					}
					if ok {
						var prior int64
						count, prior, err = decodeAggregate(old)
						if err != nil {
							return err
						}
						if prior > timestamp {
							timestamp = prior
						}
					} else {
						expiry.add(windowExpiry{end: end, key: storageKey})
					}
					count++
					task.StorePut(store, storageKey, encodeAggregate(count, timestamp))
					if err := next(record.Record[window.Key[K], int64]{
						Key:   window.Key[K]{Key: r.Key, Start: start, End: end},
						Value: count, Timestamp: timestamp,
					}); err != nil {
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
	return KStream[window.Key[K], int64]{s.builder, p, name, serde.JSON[window.Key[K]](), serde.Int64, true}
}
