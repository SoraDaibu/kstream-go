package stream

import (
	"errors"

	"github.com/SoraDaibu/kstream-go/internal/graph"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/topology"
)

// Filter materializes matching table updates. Rejected updates become deletes;
// input tombstones bypass the predicate. Keys and values are read-only.
func (t KTable[K, V]) Filter(store string, predicate func(K, V) bool) KTable[K, V] {
	t.updates.builder.mutable()
	if predicate == nil {
		t.updates.builder.fail(errors.New("table filter requires predicate"))
	}
	return transformTable(t, store, "filter", t.updates.value, func(k K, v V) (V, bool) { return v, !predicate(k, v) })
}

// FilterNot materializes nonmatching updates and deletes matching rows.
// Tombstones bypass the predicate and delete the derived row.
func (t KTable[K, V]) FilterNot(store string, predicate func(K, V) bool) KTable[K, V] {
	t.updates.builder.mutable()
	if predicate == nil {
		t.updates.builder.fail(errors.New("table filter requires predicate"))
	}
	return transformTable(t, store, "filter-not", t.updates.value, func(k K, v V) (V, bool) { return v, predicate(k, v) })
}

// MapTableValues materializes nullable (value, null) mapping results while
// preserving keys. Input tombstones delete without invoking the mapper.
func MapTableValues[K, V, O any](t KTable[K, V], store string, value serde.Codec[O], mapper func(K, V) (O, bool)) KTable[K, O] {
	return transformTable(t, store, "map-values", value, mapper)
}

func transformTable[K, V, O any](t KTable[K, V], store, kind string, value serde.Codec[O], mapper func(K, V) (O, bool)) KTable[K, O] {
	s := t.updates
	s.builder.mutable()
	if store == "" || mapper == nil || !value.Valid() {
		s.builder.fail(errors.New("table transform requires store, function and codec"))
	}
	p := &graph.Port[K, O]{}
	name := store + "-" + kind
	s.builder.add(topology.Node{Name: name, Kind: "table-" + kind, Parents: []string{s.name}, Store: store})
	s.port.Subscribe(func(ctx *graph.Context) processor.Handler[K, V] {
		task := ctx.Task
		next := p.Compile(ctx)
		return func(r record.Record[K, V]) error {
			if r.NullKey {
				return nil
			}
			var result O
			null := r.NullValue
			if !null {
				result, null = mapper(r.Key, r.Value)
			}
			key, err := s.key.Encode(r.Key)
			if err != nil {
				return err
			}
			if key == nil {
				return errors.New("non-null table key encoded as nil")
			}
			var encoded []byte
			if !null {
				encoded, err = value.Encode(result)
				if err != nil {
					return err
				}
				if encoded == nil {
					return errors.New("non-null table value encoded as nil")
				}
			}
			task.StorePut(store, string(key), encoded)
			return next(record.Record[K, O]{Key: r.Key, Value: result, NullValue: null, Timestamp: r.Timestamp})
		}
	})
	return KTable[K, O]{KStream[K, O]{s.builder, p, name, s.key, value, s.partitioned}, store, value}
}
