package stream_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
	"github.com/SoraDaibu/kstream-go/window"
)

// BenchmarkWindowReduce uses sum-of-ones with an independently derived checksum.
// It isolates typed reduction/state bookkeeping in one long-lived window and
// includes serialization/task transactions, but no broker or durable backend.
func BenchmarkWindowReduce(b *testing.B) {
	for _, keys := range []int{64, 4096} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			builder := stream.NewStreamsBuilder()
			stream.WindowedReduce(stream.Stream(builder, "in", serde.String, serde.Int64), "reduced", window.Tumbling{Size: 1 << 60},
				func(old, v int64, _ bool) (int64, bool) { return old + v, false }).To("out")
			plan, err := builder.Build()
			if err != nil {
				b.Fatal(err)
			}
			backend := &windowBenchmarkBackend{}
			a, err := plan.NewTask(ctx, execution.Spec{ApplicationID: "reduce-bench", ID: "0"}, backend)
			if err != nil {
				b.Fatal(err)
			}
			defer a.Close()
			encoded := make([][]byte, keys)
			value, _ := serde.Int64.Encode(1)
			for i := range encoded {
				encoded[i] = []byte(strconv.Itoa(i))
				if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: int64(i), Key: encoded[i], Value: value}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: int64(keys + i), Timestamp: int64(i + 1), Key: encoded[i%keys], Value: value}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			q, remainder := int64(b.N/keys), int64(b.N%keys)
			want := int64(keys) + int64(keys)*q*(q+3)/2 + remainder*(q+2)
			if backend.checksum != want {
				b.Fatal("window reduce checksum mismatch", backend.checksum, want)
			}
		})
	}
}
