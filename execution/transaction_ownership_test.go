package execution_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
)

type retainedTransactionBackend struct {
	*memory.Backend
	transactions []execution.Transaction
}

func (b *retainedTransactionBackend) Commit(ctx context.Context, spec execution.Spec, generation uint64, tx execution.Transaction) error {
	b.transactions = append(b.transactions, tx)
	return b.Backend.Commit(ctx, spec, generation, tx)
}

func TestPrivateTransactionDescriptorDoesNotReuseRetainedBuffers(t *testing.T) {
	b := &retainedTransactionBackend{Backend: memory.New()}
	a, err := execution.New(ctx, execution.Spec{ApplicationID: "retained", ID: "0"}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	clock := a.ScopedContext("stage")
	if err := a.Register("in", func(r record.Bytes) error {
		clock.Observe(r.Timestamp)
		a.StorePut("s", "k", r.Value)
		r.Topic = "out"
		a.Emit(r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		value := []byte{byte(i)}
		if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: int64(i), Timestamp: int64(i + 1), Value: value}); err != nil {
			t.Fatal(err)
		}
		value[0] = 255
	}
	// A replay-only transaction and close also clear the task's descriptor.
	if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: 0, Timestamp: 999}); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if len(b.transactions) != 4 {
		t.Fatal("unexpected durable decisions", len(b.transactions))
	}
	for i, tx := range b.transactions {
		stamp := int64(i + 1)
		if tx.StreamTime != stamp || !reflect.DeepEqual(tx.ScopeTimes, map[string]int64{"stage": stamp}) || !reflect.DeepEqual(tx.NextOffsets, map[string]int64{"in": stamp}) {
			t.Fatal("later transaction changed retained scalar/map metadata", i, tx)
		}
		if len(tx.Mutations) != 1 || len(tx.Outputs) != 1 || !reflect.DeepEqual(tx.Mutations[0].Value, []byte{byte(i)}) || !reflect.DeepEqual(tx.Outputs[0].Value, []byte{byte(i)}) || tx.Outputs[0].Timestamp != stamp {
			t.Fatal("later transaction reused retained output/state buffers", i, tx)
		}
	}
}
