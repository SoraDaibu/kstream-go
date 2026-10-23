package stream

import (
	"encoding/binary"
	"errors"

	"github.com/SoraDaibu/kstream-go/execution"
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
	p := &port[K, int64]{}
	name := store + "-count"
	s.builder.add(topology.Node{Name: name, Kind: "count", Parents: []string{s.name}, Store: store})
	s.port.subscribers = append(s.port.subscribers, func(t *execution.Task) processor.Handler[K, V] {
		next := p.compile(t)
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
			if previous, ok := t.StoreGet(store, string(key)); ok {
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
			t.StorePut(store, string(key), encodeAggregate(count, timestamp))
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
	if s.builder != table.updates.builder {
		s.builder.fail(errors.New("join inputs must share a builder"))
	}
	if !s.partitioned {
		s.builder.fail(errors.New("join requires repartition after key changes"))
	}
	if joiner == nil || !value.Valid() {
		s.builder.fail(errors.New("join requires function and codec"))
	}
	p := &port[K, O]{}
	s.builder.add(topology.Node{Name: name, Kind: "stream-table-join", Parents: []string{s.name, table.updates.name}})
	s.port.subscribers = append(s.port.subscribers, func(t *execution.Task) processor.Handler[K, L] {
		next := p.compile(t)
		return func(r record.Record[K, L]) error {
			if r.NullKey || r.NullValue {
				return nil
			}
			key, err := table.updates.key.Encode(r.Key)
			if err != nil {
				return err
			}
			if key == nil {
				return errors.New("non-null key encoded as nil")
			}
			stored, ok := t.StoreGet(table.store, string(key))
			if !ok {
				return nil
			}
			v, err := table.lookup.Decode(stored)
			if err != nil {
				return err
			}
			return next(record.Record[K, O]{Key: r.Key, Value: joiner(r.Value, v), Timestamp: r.Timestamp})
		}
	})
	return KStream[K, O]{s.builder, p, name, s.key, value, true}
}
