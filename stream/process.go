package stream

import (
	"errors"

	"github.com/SoraDaibu/kstream-go/internal/graph"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/topology"
)

// Process connects a task-local processor with independent input/output types.
// Output codecs are required. Unlike the same-type method, this function always
// invalidates key partitioning; key-local operations require repartitioning.
// The supplier may emit zero or more records with explicit timestamps and nulls.
func Process[KIn, VIn, KOut, VOut any](s KStream[KIn, VIn], name string, key serde.Codec[KOut], value serde.Codec[VOut], supplier processor.Supplier[KIn, VIn, KOut, VOut]) KStream[KOut, VOut] {
	s.builder.mutable()
	s.builder.recordMetadata = true
	if supplier == nil || !key.Valid() || !value.Valid() {
		s.builder.fail(errors.New("process requires supplier and valid output codecs"))
	}
	p := &graph.Port[KOut, VOut]{}
	s.builder.add(topology.Node{Name: name, Kind: "processor", Parents: []string{s.name}})
	s.port.Subscribe(func(ctx *graph.Context) processor.Handler[KIn, VIn] {
		return supplier(ctx.Task, processor.Forward[KOut, VOut](p.Compile(ctx)))
	})
	return KStream[KOut, VOut]{s.builder, p, name, key, value, false}
}
