package pebble_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	adapter "github.com/SoraDaibu/kstream-go/adapters/pebble"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func TestTaskRestoresCommittedStateIntoPersistentNamespace(t *testing.T) {
	engine, err := adapter.Open(filepath.Join(t.TempDir(), "state"), adapter.Config{SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	builder := stream.NewStreamsBuilder()
	stream.Count(stream.Stream(builder, "in", serde.String, serde.String), "counts").ToStream().To("out")
	plan, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	backend := memory.New()
	spec := execution.Spec{ApplicationID: "persistent", ID: "0"}
	ctx := context.Background()
	task, err := plan.NewTask(ctx, spec, backend, execution.WithStoreFactory(engine.Task))
	if err != nil {
		t.Fatal(err)
	}
	rows := []record.Bytes{{Topic: "in", Offset: 0, Key: []byte("a"), Value: []byte("x")}, {Topic: "in", Offset: 1, Key: []byte("a"), Value: []byte("x")}}
	if err = task.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if _, err = plan.NewTask(ctx, spec, backend, execution.WithStoreFactory(engine.Task)); err == nil {
		t.Fatal("persistent scope admitted two task owners")
	}
	next := rows[0]
	next.Offset = 2
	if err = task.Process(ctx, next); err != nil {
		t.Fatal("failed second initialization fenced original owner", err)
	}
	if err = task.Close(); err != nil {
		t.Fatal(err)
	}
	task, err = plan.NewTask(ctx, spec, backend, execution.WithStoreFactory(engine.Task))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err = task.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	next.Offset = 3
	if err = task.Process(ctx, next); err != nil {
		t.Fatal(err)
	}
	out := backend.Outputs(spec)
	if len(out) != 4 {
		t.Fatal("persistent replay duplicated outputs")
	}
	n, err := serde.Int64.Decode(out[3].Value)
	if err != nil || n != 4 {
		t.Fatal("persistent aggregate lost state", n, err)
	}
}
