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

// BenchmarkTypedProcessorDispatch compares the existing same-type method with
// the new cross-type function. Allocation-free scalar input codecs and no sink
// serialization isolate adapters/dispatch within the real transaction runtime.
// It does not measure durable commits, payload allocation or broker performance.
func BenchmarkTypedProcessorDispatch(b *testing.B) {
	for _, size := range []int{1, 256} {
		for _, typed := range []bool{false, true} {
			b.Run(fmt.Sprintf("batch=%d/cross-type=%t", size, typed), func(b *testing.B) {
				builder := stream.NewStreamsBuilder()
				source := stream.Stream(builder, "in", serde.Int64, serde.Int64)
				var sum uint64
				if typed {
					unsigned := serde.Codec[uint64]{
						Encode: func(v uint64) ([]byte, error) { return serde.Int64.Encode(int64(v)) },
						Decode: func(v []byte) (uint64, error) { n, err := serde.Int64.Decode(v); return uint64(n), err },
					}
					stream.Process(source, "typed", unsigned, unsigned,
						func(_ processor.Context, next processor.Forward[uint64, uint64]) processor.Handler[int64, int64] {
							return func(r record.Record[int64, int64]) error {
								return next(record.Record[uint64, uint64]{Key: uint64(r.Key), Value: uint64(r.Value), Timestamp: r.Timestamp, NullKey: r.NullKey, NullValue: r.NullValue})
							}
						}).Foreach("observe", func(r record.Record[uint64, uint64]) error { sum += r.Key + r.Value; return nil })
				} else {
					source.Process("same", true,
						func(_ processor.Context, next processor.Forward[int64, int64]) processor.Handler[int64, int64] {
							return func(r record.Record[int64, int64]) error { return next(r) }
						}).Foreach("observe", func(r record.Record[int64, int64]) error { sum += uint64(r.Key + r.Value); return nil })
				}
				plan, err := builder.Build()
				if err != nil {
					b.Fatal(err)
				}
				a, err := plan.NewTask(ctx, execution.Spec{ApplicationID: "typed-bench", ID: "0"}, &windowBenchmarkBackend{})
				if err != nil {
					b.Fatal(err)
				}
				defer a.Close()
				key, _ := serde.Int64.Encode(3)
				value, _ := serde.Int64.Encode(7)
				rows := make([]record.Bytes, size)
				for i := range rows {
					rows[i] = record.Bytes{Topic: "in", Key: key, Value: value}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for j := range rows {
						rows[j].Offset = int64(i)*int64(size) + int64(j)
					}
					if err := a.ProcessBatch(ctx, rows); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if sum != uint64(b.N)*uint64(size)*10 {
					b.Fatal("typed dispatch skipped or corrupted records", sum)
				}
			})
		}
	}
}
