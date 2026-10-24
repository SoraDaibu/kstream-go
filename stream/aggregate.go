package stream

import (
	"encoding/binary"
	"errors"

	"github.com/SoraDaibu/kstream-go/internal/graph"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/topology"
)

// KGroupedStream groups by the existing key. It does not perform a broker exchange.
type KGroupedStream[K, V any] struct{ source KStream[K, V] }

// GroupByKey requires keys that have not changed since the last partition boundary.
func (s KStream[K, V]) GroupByKey() KGroupedStream[K, V] {
	s.builder.mutable()
	if !s.partitioned {
		s.builder.fail(errors.New("groupByKey requires repartition after key changes"))
	}
	return KGroupedStream[K, V]{source: s}
}

func (g KGroupedStream[K, V]) Count(store string) KTable[K, int64] {
	return Count(g.source, store)
}

// Reduce skips null keys/values and uses the first value directly. Returning
// null=true deletes the aggregate and emits a tombstone; zero values are data.
func (g KGroupedStream[K, V]) Reduce(store string, reducer func(V, V) (value V, null bool)) KTable[K, V] {
	if reducer == nil {
		g.source.builder.fail(errors.New("reduce requires a reducer"))
	}
	return fold(g, store, "reduce", g.source.value, func(_ K, value, old V, present bool) (V, bool) {
		if !present {
			return value, false
		}
		return reducer(old, value)
	})
}

// Aggregate skips null keys/values. On a missing aggregate, initializer runs
// before aggregator. Every bool denotes null, including oldNull. A null result
// deletes state and emits a tombstone; the next record initializes again.
func Aggregate[K, V, A any](g KGroupedStream[K, V], store string, value serde.Codec[A],
	initializer func() (A, bool), aggregator func(key K, value V, old A, oldNull bool) (A, bool),
) KTable[K, A] {
	if initializer == nil || aggregator == nil {
		g.source.builder.fail(errors.New("aggregate requires initializer and aggregator"))
	}
	return fold(g, store, "aggregate", value, func(key K, input V, old A, present bool) (A, bool) {
		oldNull := false
		if !present {
			old, oldNull = initializer()
		}
		return aggregator(key, input, old, oldNull)
	})
}

func decodeFold[A any](stored []byte, codec serde.Codec[A]) (value A, timestamp int64, err error) {
	if len(stored) < 9 || stored[0] != 1 {
		return value, 0, errors.New("invalid aggregate envelope")
	}
	timestamp = int64(binary.BigEndian.Uint64(stored[1:9]))
	if timestamp < 0 {
		return value, 0, errors.New("invalid aggregate timestamp")
	}
	value, err = codec.Decode(stored[9:])
	return value, timestamp, err
}

// fold is shared typed machinery; present is an internal existence flag, not null.
func fold[K, V, A any](g KGroupedStream[K, V], store, kind string, value serde.Codec[A],
	apply func(K, V, A, bool) (A, bool),
) KTable[K, A] {
	s := g.source
	s.builder.mutable()
	if !s.partitioned || store == "" || !value.Valid() {
		s.builder.fail(errors.New("aggregation requires partitioned keys, store and valid codec"))
	}
	p := &graph.Port[K, A]{}
	name := store + "-" + kind
	s.builder.add(topology.Node{Name: name, Kind: kind, Parents: []string{s.name}, Store: store})
	s.port.Subscribe(func(t *graph.Context) processor.Handler[K, V] {
		task := t.Task
		next := p.Compile(t)
		return func(r record.Record[K, V]) error {
			if r.NullKey || r.NullValue {
				return nil
			}
			key, err := s.key.Encode(r.Key)
			if err != nil {
				return err
			}
			if key == nil {
				return errors.New("non-null key encoded as nil")
			}
			stored, present, err := task.StoreGet(store, string(key))
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
			result, null := apply(r.Key, r.Value, old, present)
			var encoded []byte
			if !null {
				encoded, err = value.Encode(result)
				if err != nil {
					return err
				}
				if encoded == nil {
					return errors.New("non-null aggregate encoded as nil")
				}
				envelope := make([]byte, 9, 9+len(encoded))
				envelope[0] = 1
				binary.BigEndian.PutUint64(envelope[1:], uint64(timestamp))
				encoded = append(envelope, encoded...)
			}
			task.StorePut(store, string(key), encoded)
			return next(record.Record[K, A]{Key: r.Key, Value: result, NullValue: null, Timestamp: timestamp})
		}
	})
	lookup := serde.Codec[A]{Encode: value.Encode, Decode: func(b []byte) (A, error) { v, _, err := decodeFold(b, value); return v, err }}
	return KTable[K, A]{updates: KStream[K, A]{s.builder, p, name, s.key, value, true}, store: store, lookup: lookup}
}
