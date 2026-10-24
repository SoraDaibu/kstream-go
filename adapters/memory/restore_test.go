package memory_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/state"
)

func TestStreamedAndLegacyRestoreAgreeAcrossBatches(t *testing.T) {
	b := memory.New()
	ctx := context.Background()
	spec := execution.Spec{ApplicationID: "restore", ID: "0"}
	generation, _, err := b.Open(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	mutations := make([]state.Mutation, 0, 700)
	for i := 0; i < 600; i++ {
		mutations = append(mutations, state.Mutation{Store: "s", Key: fmt.Sprint(i), Value: []byte(fmt.Sprint(i))})
	}
	mutations = append(mutations, state.Mutation{Store: "s", Key: "0", Value: []byte("changed")}, state.Mutation{Store: "s", Key: "1", Value: nil}, state.Mutation{Store: "s", Key: "2", Value: []byte{}})
	if err := b.Commit(ctx, spec, generation, execution.Transaction{Mutations: mutations, NextOffsets: map[string]int64{"in": 77}, StreamTime: 99}); err != nil {
		t.Fatal(err)
	}
	image := make(state.Image)
	batches := 0
	_, checkpoint, err := b.OpenInto(ctx, spec, func(rows []state.Mutation) error {
		batches++
		if len(rows) > 256 {
			t.Fatal("restore batch exceeded bound", len(rows))
		}
		for _, row := range rows {
			image.Apply(row)
		}
		return nil
	})
	if err != nil || batches != 3 || checkpoint.NextOffsets["in"] != 77 || checkpoint.StreamTime != 99 {
		t.Fatal(batches, checkpoint, err)
	}
	_, legacy, err := b.Open(ctx, spec)
	if err != nil || !reflect.DeepEqual(legacy.State, image) {
		t.Fatal("legacy/streamed state differed", err)
	}
	if _, ok := image["s"]["1"]; ok || image["s"]["2"] == nil || string(image["s"]["0"]) != "changed" {
		t.Fatal("ordered restoration lost delete/empty/update")
	}
	if err := b.Commit(ctx, spec, generation, execution.Transaction{}); !errors.Is(err, execution.ErrFenced) {
		t.Fatal("stream restore did not fence old owner", err)
	}
}

func TestRestoreSinkFailureAndCancellation(t *testing.T) {
	b := memory.New()
	ctx := context.Background()
	spec := execution.Spec{ApplicationID: "fault", ID: "0"}
	g, _, err := b.Open(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Commit(ctx, spec, g, execution.Transaction{Mutations: []state.Mutation{{Store: "s", Key: "key", Value: []byte("x")}}}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("sink failed")
	if _, _, err = b.OpenInto(ctx, spec, func([]state.Mutation) error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, _, err = b.OpenInto(ctx, spec, nil); err == nil {
		t.Fatal("nil restore sink accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err = b.OpenInto(cancelled, spec, func([]state.Mutation) error { t.Fatal("cancelled restore invoked sink"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_, snapshot, err := b.Open(ctx, spec)
	if err != nil || string(snapshot.State["s"]["key"]) != "x" {
		t.Fatal("failed restore changed committed state", err)
	}
}
