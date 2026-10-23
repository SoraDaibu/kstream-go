package execution_test

import (
	"fmt"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

// This measures Go transaction overhead, not performance relative to Kafka.
func BenchmarkTransactionBatch(b *testing.B) {
	for _, stateful := range []bool{false, true} {
		for _, size := range []int{1, 16, 256, 1024} {
			b.Run(fmt.Sprintf("stateful=%t/batch=%d", stateful, size), func(b *testing.B) {
				builder := stream.NewStreamsBuilder()
				source := stream.Stream(builder, "in", serde.String, serde.String)
				if stateful {
					stream.Count(source, "counts").ToStream().To("out")
				} else {
					source.To("out")
				}
				plan, err := builder.Build()
				if err != nil {
					b.Fatal(err)
				}
				backend := &benchBackend{}
				task, err := plan.NewTask(ctx, execution.Spec{ApplicationID: "bench", ID: "0"}, backend)
				if err != nil {
					b.Fatal(err)
				}
				rows := make([]record.Bytes, 1024)
				for i := range rows {
					rows[i] = record.Bytes{Topic: "in", Key: []byte(fmt.Sprint(i % 64)), Value: []byte("value")}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					base := backend.next
					for i := range rows {
						rows[i].Offset = base + int64(i)
					}
					for start := 0; start < len(rows); start += size {
						if err := task.ProcessBatch(ctx, rows[start:start+size]); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.ReportMetric(1024, "records/op")
			})
		}
	}
}
