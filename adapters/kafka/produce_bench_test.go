package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// resultProducer models the result-buffer and shared completion bookkeeping of
// franz-go 1.18.1 ProduceSync, with immediate callbacks instead of Kafka I/O.
// Both paths use the same producer/callback count/wait-group work. This model
// cannot measure client buffering, callback scheduling or broker throughput.
type resultProducer struct{ *immediateProducer }

func (p *resultProducer) ProduceSync(ctx context.Context, rows ...*kgo.Record) kgo.ProduceResults {
	var waiting sync.WaitGroup
	results := make(kgo.ProduceResults, 0, len(rows))
	waiting.Add(len(rows))
	callback := func(r *kgo.Record, err error) {
		results = append(results, kgo.ProduceResult{Record: r, Err: err})
		waiting.Done()
	}
	for _, r := range rows {
		p.Produce(ctx, r, callback)
	}
	waiting.Wait()
	return results
}

func BenchmarkProduceCompletion(b *testing.B) {
	for _, count := range []int{1, 2, 512, 4096} {
		for _, retain := range []bool{true, false} {
			b.Run(fmt.Sprintf("records=%d/retain-results=%t", count, retain), func(b *testing.B) {
				producer := &resultProducer{immediateProducer: &immediateProducer{}}
				rows := make([]*kgo.Record, count)
				for i := range rows {
					rows[i] = &kgo.Record{Topic: "out", Offset: int64(i)}
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var err error
					if retain {
						err = producer.ProduceSync(ctx, rows...).FirstErr()
					} else {
						err = produceAll(ctx, producer, rows)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if producer.seen != b.N*count {
					b.Fatal("completion model skipped records", producer.seen)
				}
			})
		}
	}
}
