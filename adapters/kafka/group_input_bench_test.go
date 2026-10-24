package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/twmb/franz-go/pkg/kgo"
)

// growingGroupInputs preserves the prior worker's grouping algorithm as a
// benchmark reference, excluding identical task factory/cache work on both sides.
func growingGroupInputs(rows []*kgo.Record, owned map[int32]bool) ([]execution.TaskBatch, map[int32]map[string]int64, error) {
	batches := make([]execution.TaskBatch, 0, len(owned))
	indexes := make(map[int32]int, len(owned))
	positions := make(map[int32]map[string]int64)
	for _, row := range rows {
		if !owned[row.Partition] {
			return nil, nil, errGroupRebalanced
		}
		index, ok := indexes[row.Partition]
		if !ok {
			index = len(batches)
			indexes[row.Partition] = index
			batches = append(batches, execution.TaskBatch{})
		}
		batches[index].Records = append(batches[index].Records, record.Bytes{Topic: row.Topic, Partition: row.Partition, Offset: row.Offset, Timestamp: row.Timestamp.UnixMilli(), Key: row.Key, Value: row.Value})
		if positions[row.Partition] == nil {
			positions[row.Partition] = make(map[string]int64)
		}
		positions[row.Partition][row.Topic] = row.Offset + 1
	}
	return batches, positions, nil
}

func BenchmarkGroupInputAssembly(b *testing.B) {
	for _, count := range []int{1, 256, 4096} {
		for _, layout := range []struct {
			name        string
			partitions  int
			interleaved bool
		}{{"single", 1, false}, {"contiguous", 3, false}, {"interleaved", 3, true}} {
			owned := map[int32]bool{0: true, 1: true, 2: true}
			rows := make([]*kgo.Record, count)
			for i := range rows {
				partition := i * layout.partitions / count
				if layout.interleaved {
					partition = i % layout.partitions
				}
				rows[i] = &kgo.Record{Topic: "in", Partition: int32(partition), Offset: int64(i), Timestamp: time.UnixMilli(int64(i)), Key: []byte("k"), Value: []byte("value")}
			}
			for _, packed := range []bool{false, true} {
				b.Run(fmt.Sprintf("records=%d/layout=%s/packed=%t", count, layout.name, packed), func(b *testing.B) {
					assemble := growingGroupInputs
					if packed {
						assemble = prepareGroupBatches
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						batches, positions, err := assemble(rows, owned)
						if err != nil {
							b.Fatal(err)
						}
						if len(positions) != min(count, layout.partitions) {
							b.Fatal("missing partition")
						}
						total := 0
						for _, batch := range batches {
							total += len(batch.Records)
							previous := int64(-1)
							for _, row := range batch.Records {
								if row.Offset < 0 || row.Offset >= int64(count) || row.Offset <= previous || row.Timestamp != row.Offset || row.Topic != "in" || string(row.Key) != "k" || string(row.Value) != "value" {
									b.Fatal("corrupted input record")
								}
								partition := int(row.Offset) * layout.partitions / count
								if layout.interleaved {
									partition = int(row.Offset) % layout.partitions
								}
								if row.Partition != int32(partition) {
									b.Fatal("wrong partition")
								}
								previous = row.Offset
							}
							last := batch.Records[len(batch.Records)-1]
							if positions[last.Partition]["in"] != last.Offset+1 {
								b.Fatal("missing checkpoint")
							}
						}
						if total != count {
							b.Fatal("missing/duplicated input record")
						}
					}
				})
			}
		}
	}
}
