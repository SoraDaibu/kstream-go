package stream_test

import (
	"fmt"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

// Compare native and scoped clocks with identical accepted scalar input, clock
// reads and no output serialization or durable I/O. The unrelated source exists
// only to create a second component; this is not a Kafka throughput benchmark.
func BenchmarkProcessingClockDispatch(b *testing.B) {
	for _, size := range []int{1, 256} {
		for _, scoped := range []bool{false, true} {
			b.Run(fmt.Sprintf("batch=%d/scoped=%t", size, scoped), func(b *testing.B) {
				builder := stream.NewStreamsBuilder()
				var observed int64
				stream.Stream(builder, "in", serde.Int64, serde.Int64).Process("clock", false,
					func(c processor.Context, _ processor.Forward[int64, int64]) processor.Handler[int64, int64] {
						return func(record.Record[int64, int64]) error { observed = c.StreamTime(); return nil }
					})
				if scoped {
					stream.Stream(builder, "other", serde.Int64, serde.Int64).Foreach("ignore", func(record.Record[int64, int64]) error { return nil })
				}
				plan, err := builder.Build()
				if err != nil {
					b.Fatal(err)
				}
				a, err := plan.NewTask(ctx, execution.Spec{ApplicationID: "clock-bench", ID: "0"}, &windowBenchmarkBackend{})
				if err != nil {
					b.Fatal(err)
				}
				defer a.Close()
				rows := make([]record.Bytes, size)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for j := range rows {
						stamp := int64(i)*int64(size) + int64(j)
						rows[j] = record.Bytes{Topic: "in", Offset: stamp, Timestamp: stamp}
					}
					if err := a.ProcessBatch(ctx, rows); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if observed != int64(b.N)*int64(size)-1 {
					b.Fatal("clock skipped accepted input", observed)
				}
			})
		}
	}
}
