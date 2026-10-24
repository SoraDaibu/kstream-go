package stream_test

import (
	"fmt"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func BenchmarkBranchRouting(b *testing.B) {
	const size = 256
	for _, predicates := range []int{1, 8, 64} {
		for _, match := range []string{"first", "last", "none"} {
			b.Run(fmt.Sprintf("predicates=%d/match=%s", predicates, match), func(b *testing.B) {
				builder := stream.NewStreamsBuilder()
				checks := make([]func(record.Record[int64, int64]) bool, predicates)
				for i := range checks {
					checks[i] = func(r record.Record[int64, int64]) bool { return r.Key == int64(i) }
				}
				branches := stream.Stream(builder, "in", serde.Int64, serde.Int64).Branch("route", checks...)
				var sum int64
				for i, branch := range branches {
					branch.Foreach(fmt.Sprintf("observe-%d", i), func(r record.Record[int64, int64]) error { sum += r.Value; return nil })
				}
				plan, err := builder.Build()
				if err != nil {
					b.Fatal(err)
				}
				a, err := plan.NewTask(ctx, execution.Spec{ApplicationID: "branch-bench", ID: "0"}, &windowBenchmarkBackend{})
				if err != nil {
					b.Fatal(err)
				}
				defer a.Close()
				key := int64(0)
				if match == "last" {
					key = int64(predicates - 1)
				} else if match == "none" {
					key = -1
				}
				encoded, _ := serde.Int64.Encode(key)
				value, _ := serde.Int64.Encode(7)
				rows := make([]record.Bytes, size)
				for i := range rows {
					rows[i] = record.Bytes{Topic: "in", Key: encoded, Value: value}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for j := range rows {
						rows[j].Offset = int64(i)*size + int64(j)
					}
					if err := a.ProcessBatch(ctx, rows); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				want := int64(b.N) * size * 7
				if match == "none" {
					want = 0
				}
				if sum != want {
					b.Fatal("branch delivery checksum mismatch", sum, want)
				}
			})
		}
	}
}
