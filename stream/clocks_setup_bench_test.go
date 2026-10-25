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

// Setup is outside the dispatch benchmark's timed region. Measure it separately
// so an optimization cannot hide setup memory/time proportional to scope count.
func BenchmarkProcessingClockSetup(b *testing.B) {
	for _, scopes := range []int{1, 2, 32, 256} {
		b.Run(fmt.Sprintf("scopes=%d", scopes), func(b *testing.B) {
			builder := stream.NewStreamsBuilder()
			for i := 0; i < scopes; i++ {
				stream.Stream(builder, fmt.Sprintf("in-%d", i), serde.Int64, serde.Int64).Process(fmt.Sprintf("clock-%d", i), false,
					func(c processor.Context, _ processor.Forward[int64, int64]) processor.Handler[int64, int64] {
						return func(record.Record[int64, int64]) error { _ = c.StreamTime(); return nil }
					})
			}
			plan, err := builder.Build()
			if err != nil {
				b.Fatal(err)
			}
			backend := &windowBenchmarkBackend{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a, err := plan.NewTask(ctx, execution.Spec{ApplicationID: "clock-setup", ID: "0"}, backend)
				if err != nil {
					b.Fatal(err)
				}
				if err := a.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
