package kafka

import (
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/twmb/franz-go/pkg/kgo"
)

// prepareGroupBatches preserves the poll's first-seen task order and each task's
// record order. Payload bytes remain borrowed until synchronous processing ends.
// Each exact record segment has its own capacity bound, with no cross-poll reuse.
func prepareGroupBatches(rows []*kgo.Record, owned map[int32]bool) ([]execution.TaskBatch, map[int32]map[string]int64, error) {
	capacity := min(len(owned), len(rows))
	indexes := make(map[int32]int, capacity)
	sizes := make([]int, 0, capacity)
	for _, row := range rows {
		if !owned[row.Partition] {
			return nil, nil, errGroupRebalanced
		}
		index, ok := indexes[row.Partition]
		if !ok {
			index = len(sizes)
			indexes[row.Partition] = index
			sizes = append(sizes, 0)
		}
		sizes[index]++
	}
	batches := make([]execution.TaskBatch, len(sizes))
	buffer := make([]record.Bytes, len(rows))
	start := 0
	for i, size := range sizes {
		batches[i].Records = buffer[start : start : start+size]
		start += size
	}
	positions := make(map[int32]map[string]int64, len(sizes))
	for _, row := range rows {
		index := indexes[row.Partition]
		batches[index].Records = append(batches[index].Records, record.Bytes{
			Topic: row.Topic, Partition: row.Partition, Offset: row.Offset,
			Timestamp: row.Timestamp.UnixMilli(), Key: row.Key, Value: row.Value,
		})
		if positions[row.Partition] == nil {
			positions[row.Partition] = make(map[string]int64)
		}
		positions[row.Partition][row.Topic] = row.Offset + 1
	}
	return batches, positions, nil
}
