package kafka

import (
	"context"
	"fmt"
	"testing"

	"github.com/SoraDaibu/kstream-go/record"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This session measures adapter assembly only. It inspects every record in place
// and has no network, client buffering, durable commit or result allocation.
type assemblySession struct {
	rows, outputs int64
	invalid       bool
}

func (*assemblySession) Begin() error                                             { return nil }
func (*assemblySession) End(context.Context, kgo.TransactionEndTry) (bool, error) { return true, nil }
func (s *assemblySession) ProduceSync(_ context.Context, rows ...*kgo.Record) kgo.ProduceResults {
	s.rows += int64(len(rows))
	for _, row := range rows {
		if row.Topic == "out" {
			s.outputs++
			if row.Partition < 0 || row.Partition > 1 || len(row.Key) != 1 || row.Key[0] != 'k' || string(row.Value) != "payload" || row.Timestamp.UnixMilli() != 42 {
				s.invalid = true
			}
		} else if row.Topic != "state" {
			s.invalid = true
		}
	}
	return nil
}

// BenchmarkGroupAssembly includes checkpoint encoding, record construction and
// dispatch. Correctness/ordering/EOS require the independent broker test gates.
func BenchmarkGroupAssembly(b *testing.B) {
	for _, perTask := range []int{1, 256, 2048} {
		b.Run(fmt.Sprintf("records=%d", 2*perTask), func(b *testing.B) {
			backend, _, txs := groupBackendFixture()
			session := &assemblySession{}
			backend.session = session
			for i := range txs {
				txs[i].Transaction.Outputs = make([]record.Bytes, perTask)
				for j := range txs[i].Transaction.Outputs {
					txs[i].Transaction.Outputs[j] = record.Bytes{Topic: "out", Partition: int32(i), Key: []byte("k"), Value: []byte("payload"), Timestamp: 42}
				}
			}
			b.ReportAllocs()
			b.ReportMetric(float64(2*perTask), "records/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := backend.CommitTasks(context.Background(), txs); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if session.invalid || session.outputs != int64(2*perTask*b.N) || session.rows != int64((2*perTask+4)*b.N+2) {
				b.Fatal("assembly omitted/corrupted records", session.outputs, session.rows, session.invalid)
			}
		})
	}
}
