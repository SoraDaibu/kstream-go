// Package stream exposes a minimal typed Kafka Streams-inspired DSL. The API is
// experimental. Builders and task contexts have one owner; records are immutable.
package stream

import (
	"context"
	"errors"
	"fmt"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/internal/graph"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/topology"
)

type StreamsBuilder struct {
	nodes   []topology.Node
	sources []func(*graph.Context) error
	topics  map[string]bool
	sinks   map[string]int
	err     error
	frozen  bool
}

func NewStreamsBuilder() *StreamsBuilder {
	return &StreamsBuilder{topics: make(map[string]bool), sinks: make(map[string]int)}
}
func (b *StreamsBuilder) mutable() {
	if b.frozen {
		panic("topology builder is frozen")
	}
}
func (b *StreamsBuilder) fail(err error) {
	if b.err == nil {
		b.err = err
	}
}
func (b *StreamsBuilder) add(n topology.Node) { b.mutable(); b.nodes = append(b.nodes, n) }

type Topology struct {
	plan    topology.Plan
	sources []func(*graph.Context) error
}

func (b *StreamsBuilder) Build() (Topology, error) {
	if b.frozen {
		return Topology{}, errors.New("builder already frozen")
	}
	b.frozen = true
	if b.err != nil {
		return Topology{}, b.err
	}
	if len(b.sources) == 0 {
		return Topology{}, errors.New("topology requires a source")
	}
	p, err := topology.New(b.nodes)
	if err != nil {
		return Topology{}, err
	}
	return Topology{plan: p, sources: append([]func(*graph.Context) error(nil), b.sources...)}, nil
}
func (t Topology) Describe() string    { return t.plan.Describe() }
func (t Topology) Plan() topology.Plan { return t.plan }
func (t Topology) NewTask(ctx context.Context, s execution.Spec, backend execution.Backend, options ...execution.Option) (*execution.Task, error) {
	if len(t.sources) == 0 {
		return nil, errors.New("task requires a built topology with sources")
	}
	task, err := execution.New(ctx, s, backend, options...)
	if err != nil {
		return nil, err
	}
	initialized := false
	defer func() {
		if !initialized {
			_ = task.Close()
		}
	}()
	compiled := graph.New(task)
	for _, source := range t.sources {
		if err := source(compiled); err != nil {
			return nil, err
		}
	}
	initialized = true
	return task, nil
}

type KStream[K, V any] struct {
	builder     *StreamsBuilder
	port        *graph.Port[K, V]
	name        string
	key         serde.Codec[K]
	value       serde.Codec[V]
	partitioned bool
}

func Stream[K, V any](b *StreamsBuilder, topic string, key serde.Codec[K], value serde.Codec[V]) KStream[K, V] {
	b.mutable()
	if topic == "" || b.topics[topic] {
		b.fail(fmt.Errorf("empty or duplicate topic %q", topic))
	}
	b.topics[topic] = true
	if !key.Valid() || !value.Valid() {
		b.fail(errors.New("source requires valid codecs"))
	}
	p := &graph.Port[K, V]{}
	s := KStream[K, V]{b, p, topic, key, value, true}
	b.add(topology.Node{Name: topic, Kind: "source"})
	b.sources = append(b.sources, func(t *graph.Context) error {
		h := p.Compile(t)
		return t.Task.Register(topic, func(in record.Bytes) error {
			r := record.Record[K, V]{Timestamp: in.Timestamp, NullKey: in.Key == nil, NullValue: in.Value == nil}
			var err error
			if !r.NullKey {
				r.Key, err = key.Decode(in.Key)
				if err != nil {
					return fmt.Errorf("decode key: %w", err)
				}
			}
			if !r.NullValue {
				r.Value, err = value.Decode(in.Value)
				if err != nil {
					return fmt.Errorf("decode value: %w", err)
				}
			}
			return h(r)
		})
	})
	return s
}
func (s KStream[K, V]) Process(name string, changesKeys bool, f processor.Factory[K, V]) KStream[K, V] {
	s.builder.mutable()
	if f == nil {
		s.builder.fail(errors.New("nil processor factory"))
	}
	p := &graph.Port[K, V]{}
	s.builder.add(topology.Node{Name: name, Kind: "processor", Parents: []string{s.name}})
	s.port.Subscribe(func(t *graph.Context) processor.Handler[K, V] {
		return f(t.Task, processor.Forward[K, V](p.Compile(t)))
	})
	return KStream[K, V]{s.builder, p, name, s.key, s.value, s.partitioned && !changesKeys}
}
func (s KStream[K, V]) Filter(name string, predicate func(record.Record[K, V]) bool) KStream[K, V] {
	if predicate == nil {
		s.builder.fail(errors.New("nil predicate"))
	}
	out := s.Process(name, false, func(_ processor.Context, next processor.Forward[K, V]) processor.Handler[K, V] {
		return func(r record.Record[K, V]) error {
			if predicate(r) {
				return next(r)
			}
			return nil
		}
	})
	s.builder.nodes[len(s.builder.nodes)-1].Kind = "filter"
	return out
}

// Map changes key/value types. Go methods cannot declare new type parameters.
// Any Map conservatively requires repartitioning before a key-local state operation.
func Map[K, V, K2, V2 any](s KStream[K, V], name string, key serde.Codec[K2], value serde.Codec[V2], fn func(record.Record[K, V]) record.Record[K2, V2]) KStream[K2, V2] {
	s.builder.mutable()
	if fn == nil || !key.Valid() || !value.Valid() {
		s.builder.fail(errors.New("map requires function and valid codecs"))
	}
	p := &graph.Port[K2, V2]{}
	s.builder.add(topology.Node{Name: name, Kind: "map", Parents: []string{s.name}})
	s.port.Subscribe(func(t *graph.Context) processor.Handler[K, V] {
		next := p.Compile(t)
		return func(r record.Record[K, V]) error { return next(fn(r)) }
	})
	return KStream[K2, V2]{s.builder, p, name, key, value, false}
}
func (s KStream[K, V]) To(topic string) {
	s.builder.mutable()
	s.builder.sinks[topic]++
	name := topic + "-sink"
	if n := s.builder.sinks[topic]; n > 1 {
		name = fmt.Sprintf("%s-%d", name, n)
	}
	s.builder.add(topology.Node{Name: name, Kind: "sink", Parents: []string{s.name}})
	if topic == "" {
		s.builder.fail(errors.New("sink topic required"))
	}
	s.port.Subscribe(func(t *graph.Context) processor.Handler[K, V] {
		task := t.Task
		return func(r record.Record[K, V]) error {
			out := record.Bytes{Topic: topic, Timestamp: r.Timestamp}
			var err error
			if !r.NullKey {
				out.Key, err = s.key.Encode(r.Key)
				if err != nil {
					return err
				}
				if out.Key == nil {
					return errors.New("codec encoded non-null key as nil")
				}
			}
			if !r.NullValue {
				out.Value, err = s.value.Encode(r.Value)
				if err != nil {
					return err
				}
				if out.Value == nil {
					return errors.New("codec encoded non-null value as nil")
				}
			}
			task.Emit(out)
			return nil
		}
	})
}

// Repartition records an explicit broker exchange. Build rejects this until an
// adapter can materialize the sink/source subtopology, avoiding false local routing.
func (s KStream[K, V]) Repartition(name string) KStream[K, V] {
	out := s.Process(name, false, func(_ processor.Context, next processor.Forward[K, V]) processor.Handler[K, V] {
		return processor.Handler[K, V](next)
	})
	s.builder.nodes[len(s.builder.nodes)-1].Kind = "repartition"
	out.partitioned = true
	return out
}
