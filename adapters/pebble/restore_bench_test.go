package pebble_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	adapter "github.com/SoraDaibu/kstream-go/adapters/pebble"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/state"
)

// imageOnlyBackend deliberately hides the optional streaming capability.
type imageOnlyBackend struct{ execution.Backend }

// BenchmarkRestoreIntoPebble compares identical cache loads with/without an
// intermediate image. Engine startup and model-log creation are not timed.
// There is no Kafka transport; this cannot qualify production restore latency.
func BenchmarkRestoreIntoPebble(b *testing.B) {
	for _, entries := range []int{1024, 32768} {
		for _, syncWrites := range []bool{false, true} {
			for _, streaming := range []bool{false, true} {
				b.Run(fmt.Sprintf("entries=%d/sync=%t/stream=%t", entries, syncWrites, streaming), func(b *testing.B) {
					ctx := context.Background()
					spec := execution.Spec{ApplicationID: "restore-benchmark", ID: "0"}
					model := memory.New()
					generation, _, err := model.Open(ctx, spec)
					if err != nil {
						b.Fatal(err)
					}
					rows := make([]state.Mutation, entries)
					value := bytes.Repeat([]byte{'x'}, 64)
					for i := range rows {
						rows[i] = state.Mutation{Store: "store", Key: fmt.Sprintf("key-%08d", i), Value: value}
					}
					if err := model.Commit(ctx, spec, generation, execution.Transaction{Mutations: rows, NextOffsets: map[string]int64{"in": int64(entries)}, StreamTime: 99}); err != nil {
						b.Fatal(err)
					}
					engine, err := adapter.Open(filepath.Join(b.TempDir(), "state"), adapter.Config{SyncWrites: syncWrites})
					if err != nil {
						b.Fatal(err)
					}
					defer engine.Close()
					var backend execution.Backend = model
					if !streaming {
						backend = imageOnlyBackend{model}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						task, err := execution.New(ctx, spec, backend, execution.WithStoreFactory(engine.Task))
						if err != nil {
							b.Fatal(err)
						}
						got, ok, err := task.StoreGet("store", rows[len(rows)-1].Key)
						if err != nil || !ok || !bytes.Equal(got, value) || task.StreamTime() != 99 {
							b.Fatal("incomplete cache restore", err)
						}
						if err := task.Close(); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
				})
			}
		}
	}
}
