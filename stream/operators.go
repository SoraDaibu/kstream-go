package stream

import (
	"errors"

	"github.com/SoraDaibu/kstream-go/internal/graph"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/topology"
)

func (s KStream[K, V]) FilterNot(name string, predicate func(record.Record[K, V]) bool) KStream[K, V] {
	if predicate == nil {
		s.builder.fail(errors.New("nil predicate"))
	}
	out := s.Filter(name, func(r record.Record[K, V]) bool { return !predicate(r) })
	s.builder.nodes[len(s.builder.nodes)-1].Kind = "filter-not"
	return out
}

// Peek observes each record and forwards it unchanged. External effects inside
// action are not rolled back with the task transaction and are outside EOS.
func (s KStream[K, V]) Peek(name string, action func(record.Record[K, V]) error) KStream[K, V] {
	if action == nil {
		s.builder.fail(errors.New("nil observer"))
	}
	out := s.Process(name, false, func(_ processor.Context, next processor.Forward[K, V]) processor.Handler[K, V] {
		return func(r record.Record[K, V]) error {
			if err := action(r); err != nil {
				return err
			}
			return next(r)
		}
	})
	s.builder.nodes[len(s.builder.nodes)-1].Kind = "peek"
	return out
}

// Foreach terminates this branch. Its external effects are outside transactions.
func (s KStream[K, V]) Foreach(name string, action func(record.Record[K, V]) error) {
	if action == nil {
		s.builder.fail(errors.New("nil action"))
	}
	s.Process(name, false, func(_ processor.Context, _ processor.Forward[K, V]) processor.Handler[K, V] {
		return processor.Handler[K, V](action)
	})
	s.builder.nodes[len(s.builder.nodes)-1].Kind = "foreach"
}

// MapValues preserves keys, timestamps and partitioning. The bool arguments and
// return value indicate Kafka nulls, allowing a tombstone to become a value or
// vice versa without conflating null with a Go zero value.
func MapValues[K, V, O any](s KStream[K, V], name string, value serde.Codec[O], mapper func(V, bool) (O, bool)) KStream[K, O] {
	s.builder.mutable()
	if mapper == nil || !value.Valid() {
		s.builder.fail(errors.New("mapValues requires function and valid codec"))
	}
	p := &graph.Port[K, O]{}
	s.builder.add(topology.Node{Name: name, Kind: "map-values", Parents: []string{s.name}})
	s.port.Subscribe(func(ctx *graph.Context) processor.Handler[K, V] {
		next := p.Compile(ctx)
		return func(r record.Record[K, V]) error {
			value, null := mapper(r.Value, r.NullValue)
			return next(record.Record[K, O]{Key: r.Key, Value: value, Timestamp: r.Timestamp, NullKey: r.NullKey, NullValue: null})
		}
	})
	return KStream[K, O]{s.builder, p, name, s.key, value, s.partitioned}
}

// SelectKey changes only the key. The returned bool indicates a null key.
// Stateful key-local operations require repartitioning afterward.
func SelectKey[K, V, K2 any](s KStream[K, V], name string, key serde.Codec[K2], selector func(record.Record[K, V]) (K2, bool)) KStream[K2, V] {
	if selector == nil {
		s.builder.fail(errors.New("nil key selector"))
	}
	out := Map(s, name, key, s.value, func(r record.Record[K, V]) record.Record[K2, V] {
		key, null := selector(r)
		return record.Record[K2, V]{Key: key, Value: r.Value, Timestamp: r.Timestamp, NullKey: null, NullValue: r.NullValue}
	})
	s.builder.nodes[len(s.builder.nodes)-1].Kind = "select-key"
	return out
}

// FlatMap synchronously emits zero or more typed records without requiring an
// intermediate slice. The mapper must not retain forward or call it concurrently.
// Key-local state operations require repartitioning afterward.
func FlatMap[K, V, K2, V2 any](s KStream[K, V], name string, key serde.Codec[K2], value serde.Codec[V2], mapper func(record.Record[K, V], processor.Forward[K2, V2]) error) KStream[K2, V2] {
	s.builder.mutable()
	if mapper == nil || !key.Valid() || !value.Valid() {
		s.builder.fail(errors.New("flatMap requires function and valid codecs"))
	}
	p := &graph.Port[K2, V2]{}
	s.builder.add(topology.Node{Name: name, Kind: "flat-map", Parents: []string{s.name}})
	s.port.Subscribe(func(ctx *graph.Context) processor.Handler[K, V] {
		forward := processor.Forward[K2, V2](p.Compile(ctx))
		return func(r record.Record[K, V]) error { return mapper(r, forward) }
	})
	return KStream[K2, V2]{s.builder, p, name, key, value, false}
}

// Merge preserves arrival order at the task. Inputs must share a builder and
// deployment partitioning. A shared downstream operator is instantiated once
// per task, even when several sources reach it.
func Merge[K, V any](left, right KStream[K, V], name string) KStream[K, V] {
	left.builder.mutable()
	if left.builder != right.builder {
		left.builder.fail(errors.New("merge inputs must share a builder"))
	}
	p := &graph.Port[K, V]{}
	left.builder.add(topology.Node{Name: name, Kind: "merge", Parents: []string{left.name, right.name}})
	factory := func(ctx *graph.Context) processor.Handler[K, V] { return p.Compile(ctx) }
	left.port.Subscribe(factory)
	right.port.Subscribe(factory)
	return KStream[K, V]{left.builder, p, name, left.key, left.value, left.partitioned && right.partitioned}
}

// FlatMapValues emits zero or more values while preserving key, timestamp and
// partitioning. The mapper and yield calls are synchronous; retaining yield is
// an error. The yield closure is created once per task, not once per record.
func FlatMapValues[K, V, O any](s KStream[K, V], name string, value serde.Codec[O], mapper func(V, bool, func(O, bool) error) error) KStream[K, O] {
	s.builder.mutable()
	if mapper == nil || !value.Valid() {
		s.builder.fail(errors.New("flatMapValues requires function and valid codec"))
	}
	p := &graph.Port[K, O]{}
	s.builder.add(topology.Node{Name: name, Kind: "flat-map-values", Parents: []string{s.name}})
	s.port.Subscribe(func(ctx *graph.Context) processor.Handler[K, V] {
		next := p.Compile(ctx)
		var current record.Record[K, V]
		var firstError error
		active := false
		yield := func(value O, null bool) error {
			if !active {
				return errors.New("value forward used outside mapper")
			}
			if firstError != nil {
				return firstError
			}
			firstError = next(record.Record[K, O]{Key: current.Key, Value: value, Timestamp: current.Timestamp, NullKey: current.NullKey, NullValue: null})
			return firstError
		}
		return func(r record.Record[K, V]) error {
			current = r
			firstError = nil
			active = true
			defer func() { active = false; current = record.Record[K, V]{} }()
			err := mapper(r.Value, r.NullValue, yield)
			if err != nil {
				return err
			}
			return firstError
		}
	})
	return KStream[K, O]{s.builder, p, name, s.key, value, s.partitioned}
}
