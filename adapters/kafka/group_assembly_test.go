package kafka

import (
	"context"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/record"
	"github.com/twmb/franz-go/pkg/kgo"
)

type retainingGroupSession struct {
	fakeGroupSession
	retained [][]*kgo.Record
}

func (s *retainingGroupSession) ProduceSync(ctx context.Context, rows ...*kgo.Record) kgo.ProduceResults {
	// Retain the original pointers as a client hook might, instead of clones.
	s.retained = append(s.retained, append([]*kgo.Record(nil), rows...))
	return s.fakeGroupSession.ProduceSync(ctx, rows...)
}

func TestGroupAssemblyOwnsDistinctRecordsAcrossCalls(t *testing.T) {
	backend, _, txs := groupBackendFixture()
	session := &retainingGroupSession{fakeGroupSession: fakeGroupSession{committed: true}}
	backend.session = session
	txs[0].Transaction.Outputs = []record.Bytes{
		{Topic: "out", Key: nil, Value: []byte{}, Timestamp: 42},
		{Topic: "out", Key: []byte{}, Value: nil, Timestamp: 43},
		{Topic: "out", Key: []byte("key"), Value: []byte("first"), Timestamp: 44},
	}
	if err := backend.CommitTasks(context.Background(), txs); err != nil {
		t.Fatal(err)
	}
	first := session.retained[0]
	snapshots := make([]kgo.Record, len(first))
	for i, row := range first {
		snapshots[i] = *row
	}
	var outputs []*kgo.Record
	for _, row := range first {
		if row.Topic == "out" && row.Partition == 0 {
			outputs = append(outputs, row)
		}
	}
	if len(outputs) != 3 || outputs[0].Key != nil || outputs[0].Value == nil || outputs[1].Key == nil || outputs[1].Value != nil || string(outputs[2].Value) != "first" || outputs[2].Timestamp.UnixMilli() != 44 {
		t.Fatal("assembly changed order/null/empty/timestamp", outputs)
	}
	txs[0].Transaction.Outputs = []record.Bytes{{Topic: "other", Value: []byte("second"), Timestamp: 99}}
	txs[0].Transaction.NextOffsets = map[string]int64{"in": 2}
	if err := backend.CommitTasks(context.Background(), txs); err != nil {
		t.Fatal(err)
	}
	seen := make(map[*kgo.Record]bool)
	for _, call := range session.retained {
		for _, row := range call {
			if row == nil || seen[row] {
				t.Fatal("record structs aliased or reused across dispatch", row)
			}
			seen[row] = true
		}
	}
	for i, row := range first {
		if !reflect.DeepEqual(*row, snapshots[i]) {
			t.Fatal("a later commit mutated a retained client record", i)
		}
	}
}
