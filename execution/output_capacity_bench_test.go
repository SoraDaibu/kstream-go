package execution_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/state"
)

type outputCapacityBackend struct {
	next  int64
	shape string
	size  int
}

func (*outputCapacityBackend) Open(context.Context, execution.Spec) (uint64, execution.Snapshot, error) {
	return 1, execution.Snapshot{State: make(state.Image), StreamTime: -1}, nil
}

// Independently enumerate the expected output sequence for every transaction.
func (b *outputCapacityBackend) Commit(_ context.Context, _ execution.Spec, _ uint64, tx execution.Transaction) error {
	next := tx.NextOffsets["in"]
	if next-b.next != int64(b.size) {
		return errors.New("missing processed input")
	}
	output := 0
	for id := b.next; id < next; id++ {
		copies := 1
		switch b.shape {
		case "none":
			copies = 0
		case "sparse":
			if id&63 != 0 {
				copies = 0
			}
		case "fanout":
			copies = 3
		case "bursty":
			if (b.next/int64(b.size))&1 != 0 && id&63 != 0 {
				copies = 0
			}
		}
		for i := 0; i < copies; i++ {
			if output >= len(tx.Outputs) {
				return errors.New("missing output")
			}
			r := tx.Outputs[output]
			output++
			if r.Topic != "out" || r.Partition != 0 || r.Offset != -1 || r.Timestamp != id || len(r.Key) != 1 || r.Key[0] != 'k' || len(r.Value) != 1 || r.Value[0] != 'v' {
				return errors.New("invalid output sequence/payload")
			}
		}
	}
	if output != len(tx.Outputs) {
		return errors.New("extra output")
	}
	b.next = next
	return nil
}

func BenchmarkOutputCapacity(b *testing.B) {
	for _, size := range []int{256, 4096} {
		for _, shape := range []string{"dense", "sparse", "none", "fanout", "bursty"} {
			capacities := []int{0, size}
			if shape == "sparse" {
				capacities = append(capacities, size/64)
			}
			if shape == "fanout" {
				capacities = append(capacities, 3*size)
			}
			for _, capacity := range capacities {
				b.Run(fmt.Sprintf("batch=%d/shape=%s/capacity=%d", size, shape, capacity), func(b *testing.B) {
					backend := &outputCapacityBackend{size: size, shape: shape}
					a, err := execution.New(ctx, execution.Spec{ApplicationID: "output-capacity-bench", ID: "0"}, backend, execution.WithOutputCapacity(capacity))
					if err != nil {
						b.Fatal(err)
					}
					defer a.Close()
					calls := int64(0)
					if err := a.Register("in", func(r record.Bytes) error {
						calls++
						if shape == "none" || (shape == "sparse" && r.Offset%64 != 0) || (shape == "bursty" && (r.Offset/int64(size))%2 != 0 && r.Offset%64 != 0) {
							return nil
						}
						copies := 1
						if shape == "fanout" {
							copies = 3
						}
						r.Topic = "out"
						for i := 0; i < copies; i++ {
							a.Emit(r)
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					rows := make([]record.Bytes, size)
					key, value := []byte("k"), []byte("v")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for j := range rows {
							rows[j] = record.Bytes{Topic: "in", Offset: backend.next + int64(j), Timestamp: backend.next + int64(j), Key: key, Value: value}
						}
						if err := a.ProcessBatch(ctx, rows); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if calls != int64(b.N)*int64(size) || calls != backend.next {
						b.Fatal("missing input callbacks", calls, backend.next)
					}
				})
			}
		}
	}
}
