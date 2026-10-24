package execution_test

import (
	"context"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
)

type retainedOutputBackend struct {
	*memory.Backend
	transactions [][]record.Bytes
}

func (b *retainedOutputBackend) Commit(ctx context.Context, spec execution.Spec, generation uint64, tx execution.Transaction) error {
	b.transactions = append(b.transactions, tx.Outputs)
	return b.Backend.Commit(ctx, spec, generation, tx)
}

func TestOutputCapacityDoesNotLimitFanoutOrReuseCommittedArrays(t *testing.T) {
	backend := &retainedOutputBackend{Backend: memory.New()}
	a, err := execution.New(ctx, execution.Spec{ApplicationID: "capacity", ID: "0"}, backend, execution.WithOutputCapacity(1))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Register("in", func(r record.Bytes) error {
		r.Topic = "out"
		for i := 0; i < 3; i++ {
			a.Emit(r)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	value := []byte("first")
	if err := a.Process(ctx, record.Bytes{Topic: "in", Timestamp: 5, Key: []byte{}, Value: value}); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: 1, Timestamp: 6, Key: []byte{}, Value: nil}); err != nil {
		t.Fatal(err)
	}
	if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: 2, Timestamp: 7, Key: []byte{}, Value: []byte{}}); err != nil {
		t.Fatal(err)
	}
	if len(backend.transactions) != 3 {
		t.Fatal("missing transactions")
	}
	for call, rows := range backend.transactions {
		if len(rows) != 3 {
			t.Fatal("capacity became an output limit", len(rows))
		}
		for _, r := range rows {
			if r.Topic != "out" || r.Key == nil || len(r.Key) != 0 || r.Offset != -1 || r.Timestamp != int64(5+call) {
				t.Fatal("changed retained output", call, r)
			}
			switch call {
			case 0:
				if string(r.Value) != "first" {
					t.Fatal("committed payload aliased input or later transaction", r)
				}
			case 1:
				if r.Value != nil {
					t.Fatal("lost tombstone", r)
				}
			case 2:
				if r.Value == nil || len(r.Value) != 0 {
					t.Fatal("lost empty value", r)
				}
			}
		}
	}
}

func TestOutputCapacityDoesNotAllocateAnEmptyOutputSlice(t *testing.T) {
	backend := &retainedOutputBackend{Backend: memory.New()}
	a, err := execution.New(ctx, execution.Spec{ApplicationID: "empty-capacity", ID: "0"}, backend, execution.WithOutputCapacity(65536))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Register("in", func(record.Bytes) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := a.Process(ctx, record.Bytes{Topic: "in"}); err != nil {
		t.Fatal(err)
	}
	if len(backend.transactions) != 1 || backend.transactions[0] != nil {
		t.Fatal("zero-output transaction reserved a slice")
	}
}

func TestInvalidOutputCapacityCannotFenceHealthyTask(t *testing.T) {
	backend := memory.New()
	spec := execution.Spec{ApplicationID: "capacity-fence", ID: "0"}
	healthy, err := execution.New(ctx, spec, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()
	for _, capacity := range []int{-1, 65537} {
		if _, err := execution.New(ctx, spec, backend, execution.WithOutputCapacity(capacity)); err == nil {
			t.Fatal("invalid capacity accepted", capacity)
		}
	}
	if err := healthy.Register("in", func(record.Bytes) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := healthy.Process(ctx, record.Bytes{Topic: "in"}); err != nil {
		t.Fatal("invalid setup fenced healthy owner", err)
	}
}
