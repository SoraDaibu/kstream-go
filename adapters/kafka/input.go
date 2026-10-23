package kafka

import (
	"context"
	"errors"

	"github.com/SoraDaibu/kstream-go/record"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Input reads one explicitly owned partition across selected topics. It does
// not join a consumer group or perform rebalance assignment. Recreate it after
// restoring a poisoned task; its positions begin at the committed checkpoint.
type Input struct{ client *kgo.Client }

func (b *Backend) Input(topics ...string) (*Input, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.opened || b.failed || b.closed {
		return nil, errors.New("open a healthy Kafka task before its input")
	}
	if len(topics) == 0 {
		return nil, errors.New("input topics are required")
	}
	partitions := make(map[string]map[int32]kgo.Offset, len(topics))
	for _, topic := range topics {
		if topic == "" || topic == b.config.ChangelogTopic {
			return nil, errors.New("invalid input topic")
		}
		partitions[topic] = map[int32]kgo.Offset{b.spec.Partition: kgo.NewOffset().At(b.next[topic])}
	}
	client, err := kgo.NewClient(b.options(kgo.ConsumePartitions(partitions), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		return nil, err
	}
	return &Input{client}, nil
}
func (i *Input) Next(ctx context.Context, maxRecords int) ([]record.Bytes, error) {
	if maxRecords < 1 {
		return nil, errors.New("max records must be positive")
	}
	fetches := i.client.PollRecords(ctx, maxRecords)
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, errs[0].Err
	}
	rows := make([]record.Bytes, 0, fetches.NumRecords())
	for _, r := range fetches.Records() {
		rows = append(rows, record.Bytes{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp.UnixMilli(), Key: record.CloneBytes(r.Key), Value: record.CloneBytes(r.Value)})
	}
	return rows, nil
}
func (i *Input) Close() { i.client.Close() }
