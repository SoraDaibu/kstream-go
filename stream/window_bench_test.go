package stream_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/state"
	"github.com/SoraDaibu/kstream-go/stream"
	"github.com/SoraDaibu/kstream-go/window"
)

type windowBenchmarkBackend struct{ checksum int64 }

func (*windowBenchmarkBackend) Open(context.Context, execution.Spec) (uint64, execution.Snapshot, error) {
	return 1, execution.Snapshot{State: make(state.Image), StreamTime: -1}, nil
}
func (b *windowBenchmarkBackend) Commit(_ context.Context, _ execution.Spec, _ uint64, tx execution.Transaction) error {
	for _, r := range tx.Outputs {
		n, err := serde.Int64.Decode(r.Value)
		if err != nil {
			return err
		}
		b.checksum += n
	}
	return nil
}

// BenchmarkWindowExpiry isolates window bookkeeping and transactional state.
// It includes serialization but has no durable backend or broker.
func BenchmarkWindowExpiry(b *testing.B) {
	for _, keys := range []int{64, 4096} {
		for _, size := range []int64{1024, 1 << 60} {
			b.Run(fmt.Sprintf("keys=%d/size=%d", keys, size), func(b *testing.B) {
				builder := stream.NewStreamsBuilder()
				stream.WindowedCount(stream.Stream(builder, "in", serde.String, serde.String), "windows", window.Tumbling{Size: size, Grace: size}).To("out")
				plan, err := builder.Build()
				if err != nil {
					b.Fatal(err)
				}
				backend := &windowBenchmarkBackend{}
				task, err := plan.NewTask(context.Background(), execution.Spec{ApplicationID: "window-bench", ID: "0"}, backend)
				if err != nil {
					b.Fatal(err)
				}
				defer task.Close()
				encoded := make([][]byte, keys)
				value := []byte("value")
				for i := range encoded {
					encoded[i] = []byte(strconv.Itoa(i))
					if err := task.Process(context.Background(), record.Bytes{Topic: "in", Offset: int64(i), Key: encoded[i], Value: value}); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := task.Process(context.Background(), record.Bytes{Topic: "in", Offset: int64(keys + i), Timestamp: int64(i + 1), Key: encoded[i%keys], Value: value}); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				// Independent count model outside the timed region verifies every
				// output was processed, including window rollover and prefill.
				type location struct {
					key   int
					start int64
				}
				model := make(map[location]int64)
				expected := int64(keys)
				for i := 0; i < keys; i++ {
					model[location{key: i}] = 1
				}
				for i := 0; i < b.N; i++ {
					at := location{key: i % keys, start: int64(i+1) / size * size}
					model[at]++
					expected += model[at]
				}
				if backend.checksum != expected {
					b.Fatalf("window checksum %d, expected %d", backend.checksum, expected)
				}
			})
		}
	}
}
