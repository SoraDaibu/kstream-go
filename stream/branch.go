package stream

import (
	"errors"
	"fmt"

	"github.com/SoraDaibu/kstream-go/internal/graph"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/topology"
)

// Branch routes each record to the first matching predicate's stream. Predicates
// run in argument order, including for null keys/values. Unmatched records are
// dropped. Use a final always-true predicate for a catch-all branch. Output node
// names are name-0, name-1, and so on. Keys and partition ownership are preserved.
func (s KStream[K, V]) Branch(name string, predicates ...func(record.Record[K, V]) bool) []KStream[K, V] {
	s.builder.mutable()
	if len(predicates) == 0 {
		s.builder.fail(errors.New("branch requires at least one predicate"))
	}
	// The caller may reuse its variadic slice after building. The topology owns
	// the dispatch list, while callback closures remain the caller's responsibility.
	checks := append([]func(record.Record[K, V]) bool(nil), predicates...)
	ports := make([]*graph.Port[K, V], len(checks))
	outputs := make([]KStream[K, V], len(checks))
	s.builder.add(topology.Node{Name: name, Kind: "branch", Parents: []string{s.name}})
	for i, predicate := range checks {
		if predicate == nil {
			s.builder.fail(errors.New("branch requires non-nil predicates"))
		}
		ports[i] = &graph.Port[K, V]{}
		child := fmt.Sprintf("%s-%d", name, i)
		s.builder.add(topology.Node{Name: child, Kind: "branch-output", Parents: []string{name}})
		outputs[i] = KStream[K, V]{s.builder, ports[i], child, s.key, s.value, s.partitioned}
	}
	s.port.Subscribe(func(ctx *graph.Context) processor.Handler[K, V] {
		handlers := make([]processor.Handler[K, V], len(ports))
		for i, p := range ports {
			handlers[i] = p.Compile(ctx)
		}
		return func(r record.Record[K, V]) error {
			for i, predicate := range checks {
				if predicate(r) {
					return handlers[i](r)
				}
			}
			return nil
		}
	})
	return outputs
}
