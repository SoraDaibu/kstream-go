// Package graph compiles typed operator ports into one synchronous graph per task.
// Dynamic bindings exist only during compilation; record dispatch remains typed.
package graph

import (
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
)

type Context struct {
	Task          *execution.Task
	ApplicationID string
	compiled      map[any]any
}

func New(task *execution.Task, application string) *Context {
	return &Context{Task: task, ApplicationID: application, compiled: make(map[any]any)}
}

type Port[K, V any] struct {
	subscribers []func(*Context) processor.Handler[K, V]
}

func (p *Port[K, V]) Subscribe(factory func(*Context) processor.Handler[K, V]) {
	p.subscribers = append(p.subscribers, factory)
}
func (p *Port[K, V]) Compile(ctx *Context) processor.Handler[K, V] {
	if compiled, ok := ctx.compiled[p]; ok {
		return compiled.(processor.Handler[K, V])
	}
	handlers := make([]processor.Handler[K, V], len(p.subscribers))
	// Bind before descending. Logical topology validation rejects cycles; early
	// binding also ensures a shared downstream node has exactly one task instance.
	h := processor.Handler[K, V](func(r record.Record[K, V]) error {
		for _, handler := range handlers {
			if err := handler(r); err != nil {
				return err
			}
		}
		return nil
	})
	ctx.compiled[p] = h
	for i, factory := range p.subscribers {
		handlers[i] = factory(ctx)
	}
	return h
}
