package execution_test

import (
	"fmt"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
)

// BenchmarkRecordMetadataCapture measures enabled tracking/getter use separately
// from the unchanged DSL regression workloads. It has no broker or durable state.
func BenchmarkRecordMetadataCapture(b *testing.B) {
	for _, size := range []int{1, 256} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("batch=%d/enabled=%t", size, enabled), func(b *testing.B) {
				backend := &benchBackend{}
				var options []execution.Option
				if enabled {
					options = append(options, execution.WithRecordMetadata())
				}
				a, err := execution.New(ctx, execution.Spec{ApplicationID: "metadata-bench", ID: "0"}, backend, options...)
				if err != nil {
					b.Fatal(err)
				}
				defer a.Close()
				calls := int64(0)
				if err := a.Register("in", func(r record.Bytes) error {
					meta, present := a.RecordMetadata()
					if present != enabled || (present && meta != (record.Metadata{Topic: "in", Offset: r.Offset})) {
						b.Fatal("wrong metadata", meta, present)
					}
					calls++
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				rows := make([]record.Bytes, size)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for j := range rows {
						rows[j] = record.Bytes{Topic: "in", Offset: backend.next + int64(j)}
					}
					if err := a.ProcessBatch(ctx, rows); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if calls != int64(b.N)*int64(size) || backend.next != calls {
					b.Fatal("lost metadata callbacks", calls, backend.next)
				}
			})
		}
	}
}
