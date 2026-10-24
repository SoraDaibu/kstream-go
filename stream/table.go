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

type KTable[K, V any] struct {
	updates KStream[K, V]
	store   string
	lookup  serde.Codec[V]
}

func (t KTable[K, V]) ToStream() KStream[K, V] { return t.updates }
func Table[K, V any](b *StreamsBuilder, topic, store string, key serde.Codec[K], value serde.Codec[V]) KTable[K, V] {
	return materialize(Stream(b, topic, key, value), store)
}
func materialize[K, V any](s KStream[K, V], store string) KTable[K, V] {
	if store == "" {
		s.builder.fail(errors.New("table store name required"))
	}
	out := s.Process(store+"-table", false, func(ctx processor.Context, next processor.Forward[K, V]) processor.Handler[K, V] {
		return func(r record.Record[K, V]) error {
			if r.NullKey {
				return nil
			}
			key, err := s.key.Encode(r.Key)
			if err != nil {
				return err
			}
			if key == nil {
				return errors.New("non-null key encoded as nil")
			}
			var value []byte
			if !r.NullValue {
				value, err = s.value.Encode(r.Value)
				if err != nil {
					return err
				}
				if value == nil {
					return errors.New("non-null value encoded as nil")
				}
			}
			ctx.StorePut(store, string(key), value)
			return next(r)
		}
	})
	s.builder.nodes[len(s.builder.nodes)-1].Store = store
	return KTable[K, V]{out, store, s.value}
}
func encodeAggregate(value, timestamp int64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, uint64(value))
	binary.BigEndian.PutUint64(b[8:], uint64(timestamp))
	return b
}
func decodeAggregate(b []byte) (int64, int64, error) {
	if len(b) != 16 {
		return 0, 0, errors.New("invalid aggregate state")
	}
	return int64(binary.BigEndian.Uint64(b)), int64(binary.BigEndian.Uint64(b[8:])), nil
}

// Count groups by the existing key. Key changes require an explicit repartition.
func Count[K, V any](s KStream[K, V], store string) KTable[K, int64] {
	s.builder.mutable()
	if !s.partitioned {
		s.builder.fail(errors.New("count requires repartition after key changes"))
	}
	if store == "" {
		s.builder.fail(errors.New("count store name required"))
	}
	p := &graph.Port[K, int64]{}
	name := store + "-count"
	s.builder.add(topology.Node{Name: name, Kind: "count", Parents: []string{s.name}, Store: store})
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
			count := int64(0)
			timestamp := r.Timestamp
			previous, ok, err := task.StoreGet(store, string(key))
			if err != nil {
				return err
			}
			if ok {
				var prior int64
				count, prior, err = decodeAggregate(previous)
				if err != nil {
					return err
				}
				if prior > timestamp {
					timestamp = prior
				}
			}
			count++
			task.StorePut(store, string(key), encodeAggregate(count, timestamp))
			return next(record.Record[K, int64]{Key: r.Key, Value: count, Timestamp: timestamp})
		}
	})
	// Count's internal state includes the aggregate timestamp. Its lookup codec
	// reads the count prefix; emitted values use the ordinary Kafka long codec.
	lookup := serde.Codec[int64]{Encode: serde.Int64.Encode, Decode: func(b []byte) (int64, error) { v, _, err := decodeAggregate(b); return v, err }}
	return KTable[K, int64]{KStream[K, int64]{s.builder, p, name, s.key, serde.Int64, true}, store, lookup}
}

// Join performs a nonversioned stream-table inner join. Only stream arrivals emit.
func Join[K, L, R, O any](s KStream[K, L], table KTable[K, R], name string, value serde.Codec[O], joiner func(L, R) O) KStream[K, O] {
	s.builder.mutable()
	if joiner == nil {
		s.builder.fail(errors.New("join requires function"))
	}
	return joinTable(s, table, name, value, false, func(left L, right R, _ bool) (O, bool) { return joiner(left, right), false })
}

// JoinNullable is an inner join whose joiner returns (value, null). A null result
// emits a Kafka null value; null stream keys/values and missing table rows skip.
func JoinNullable[K, L, R, O any](s KStream[K, L], table KTable[K, R], name string, value serde.Codec[O], joiner func(L, R) (O, bool)) KStream[K, O] {
	s.builder.mutable()
	if joiner == nil {
		s.builder.fail(errors.New("join requires function"))
	}
	return joinTable(s, table, name, value, false, func(left L, right R, _ bool) (O, bool) { return joiner(left, right) })
}

// LeftJoin performs current-value lookup and supplies rightNull for absent table
// rows, including a null stream key. Null stream values skip. The joiner returns
// (value, null), preserving ordinary Go zero values as non-null data.
func LeftJoin[K, L, R, O any](s KStream[K, L], table KTable[K, R], name string, value serde.Codec[O], joiner func(left L, right R, rightNull bool) (O, bool)) KStream[K, O] {
	return joinTable(s, table, name, value, true, joiner)
}

func joinTable[K, L, R, O any](s KStream[K, L], table KTable[K, R], name string, value serde.Codec[O], left bool, joiner func(L, R, bool) (O, bool)) KStream[K, O] {
	s.builder.mutable()
	if s.builder != table.updates.builder {
		s.builder.fail(errors.New("join inputs must share a builder"))
	}
	if !s.partitioned {
		s.builder.fail(errors.New("join requires repartition after key changes"))
	}
	if joiner == nil || !value.Valid() {
		s.builder.fail(errors.New("join requires function and codec"))
	}
	p := &graph.Port[K, O]{}
	kind := "stream-table-join"
	if left {
		kind = "stream-table-left-join"
	}
	s.builder.add(topology.Node{Name: name, Kind: kind, Parents: []string{s.name, table.updates.name}})
	s.port.Subscribe(func(t *graph.Context) processor.Handler[K, L] {
		task := t.Task
		next := p.Compile(t)
		return func(r record.Record[K, L]) error {
			if r.NullValue || (r.NullKey && !left) {
				return nil
			}
			var right R
			rightNull := true
			if !r.NullKey {
				key, err := table.updates.key.Encode(r.Key)
				if err != nil {
					return err
				}
				if key == nil {
					return errors.New("non-null key encoded as nil")
				}
				stored, ok, err := task.StoreGet(table.store, string(key))
				if err != nil {
					return err
				}
				if ok {
					right, err = table.lookup.Decode(stored)
					if err != nil {
						return err
					}
					rightNull = false
				}
			}
			if rightNull && !left {
				return nil
			}
			result, null := joiner(r.Value, right, rightNull)
			return next(record.Record[K, O]{Key: r.Key, NullKey: r.NullKey, Value: result, NullValue: null, Timestamp: r.Timestamp})
		}
	})
	return KStream[K, O]{s.builder, p, name, s.key, value, true}
}
