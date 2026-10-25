package execution_test

import (
	"context"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
)

type retainedClockBackend struct {
	*memory.Backend
	transactions []execution.Transaction
}

func (b *retainedClockBackend) Commit(ctx context.Context, spec execution.Spec, generation uint64, tx execution.Transaction) error {
	b.transactions = append(b.transactions, tx)
	return b.Backend.Commit(ctx, spec, generation, tx)
}

func TestScopedViewsAndCopiedHandlesSharePrefixWithoutMutatingCommittedMaps(t *testing.T) {
	b := &retainedClockBackend{Backend: memory.New()}
	a, err := execution.New(ctx, execution.Spec{ApplicationID: "shared-views", ID: "0"}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	first, second, other := a.ScopedContext("shared"), a.ScopedContext("shared"), a.ScopedContext("other")
	copied := *first
	if copied.StreamTime() != -1 {
		t.Fatal("new scope already has time")
	}
	if err := a.Register("in", func(r record.Bytes) error {
		if r.Offset%2 == 0 {
			copied.Observe(r.Timestamp)
		} else {
			first.Observe(r.Timestamp)
		}
		if first.StreamTime() != r.Timestamp || second.StreamTime() != r.Timestamp || copied.StreamTime() != r.Timestamp {
			t.Fatal("scope views disagree about accepted prefix")
		}
		other.Observe(r.Timestamp - 1)
		if second.StreamTime() != r.Timestamp || other.StreamTime() != r.Timestamp-1 {
			t.Fatal("unrelated clocks interfered")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for batch := 0; batch < 3; batch++ {
		rows := []record.Bytes{{Topic: "in", Offset: int64(batch * 2), Timestamp: int64(batch*2 + 1)}, {Topic: "in", Offset: int64(batch*2 + 1), Timestamp: int64(batch*2 + 2)}}
		if err := a.ProcessBatch(ctx, rows); err != nil {
			t.Fatal(err)
		}
	}
	for i, tx := range b.transactions {
		if tx.ScopeTimes["shared"] != int64(i*2+2) || tx.ScopeTimes["other"] != int64(i*2+1) {
			t.Fatal("later transaction rewrote retained checkpoint", i, tx.ScopeTimes)
		}
	}
}
