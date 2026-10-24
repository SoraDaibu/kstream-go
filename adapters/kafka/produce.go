package kafka

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type syncRecordProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

// Optional capability: callbacks must complete exactly once for every submitted
// record, including failures/cancellation. Existing injected sync-only sessions
// keep their original contract and fallback. The real client exposes both APIs.
type asyncRecordProducer interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
}

var _ asyncRecordProducer = (*kgo.Client)(nil)
var _ asyncRecordProducer = (*kgo.GroupTransactSession)(nil)

type produceCompletion struct {
	waiting sync.WaitGroup
	failed  sync.Once
	err     error
}

func (c *produceCompletion) complete(_ *kgo.Record, err error) {
	if err != nil {
		c.failed.Do(func() { c.err = err })
	}
	c.waiting.Done()
}

// produceAll waits for all records but retains only their first completion
// error. No transaction decision may happen while callbacks remain outstanding.
// Record arrays/payloads remain owned by the caller; no buffer is recycled here.
func produceAll(ctx context.Context, producer syncRecordProducer, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	async, ok := producer.(asyncRecordProducer)
	if !ok {
		return producer.ProduceSync(ctx, records...).FirstErr()
	}
	completion := &produceCompletion{}
	completion.waiting.Add(len(records))
	promise := completion.complete
	for _, r := range records {
		async.Produce(ctx, r, promise)
	}
	completion.waiting.Wait()
	return completion.err
}
