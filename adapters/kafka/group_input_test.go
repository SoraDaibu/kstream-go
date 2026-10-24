package kafka

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/SoraDaibu/kstream-go/record"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPreparedInputsPreserveTaskAndRecordOrderAcrossTopics(t *testing.T) {
	rows := []*kgo.Record{
		{Topic: "left", Partition: 2, Offset: 4, Timestamp: time.UnixMilli(10), Key: nil, Value: []byte{}},
		{Topic: "left", Partition: 0, Offset: 1, Timestamp: time.UnixMilli(30), Key: []byte{}, Value: nil},
		{Topic: "right", Partition: 2, Offset: 8, Timestamp: time.UnixMilli(2), Key: []byte("a"), Value: []byte("b")},
		{Topic: "right", Partition: 0, Offset: 11, Timestamp: time.UnixMilli(20), Key: nil, Value: nil},
		{Topic: "left", Partition: 2, Offset: 5, Timestamp: time.UnixMilli(11), Key: []byte("c"), Value: []byte("d")},
	}
	batches, positions, err := prepareGroupBatches(rows, map[int32]bool{0: true, 1: true, 2: true})
	if err != nil || len(batches) != 2 {
		t.Fatal(batches, err)
	}
	want := [][]record.Bytes{
		{{Topic: "left", Partition: 2, Offset: 4, Timestamp: 10, Key: nil, Value: []byte{}},
			{Topic: "right", Partition: 2, Offset: 8, Timestamp: 2, Key: []byte("a"), Value: []byte("b")},
			{Topic: "left", Partition: 2, Offset: 5, Timestamp: 11, Key: []byte("c"), Value: []byte("d")}},
		{{Topic: "left", Partition: 0, Offset: 1, Timestamp: 30, Key: []byte{}, Value: nil},
			{Topic: "right", Partition: 0, Offset: 11, Timestamp: 20, Key: nil, Value: nil}},
	}
	for i, batch := range batches {
		if batch.Task != nil || !reflect.DeepEqual(batch.Records, want[i]) || cap(batch.Records) != len(batch.Records) {
			t.Fatal("routing changed order/nulls/metadata/capacity", i, batch)
		}
	}
	if !reflect.DeepEqual(positions, map[int32]map[string]int64{2: {"left": 6, "right": 9}, 0: {"left": 2, "right": 12}}) {
		t.Fatal("prepared input omitted a topic checkpoint", positions)
	}
	// Appending to one task must not overwrite the following task's segment.
	batches[0].Records = append(batches[0].Records, record.Bytes{Timestamp: 999})
	if !reflect.DeepEqual(batches[1].Records, want[1]) {
		t.Fatal("task record capacity crossed partition boundary")
	}
}

func TestPreparedInputsRejectAnyUnownedPartition(t *testing.T) {
	rows := []*kgo.Record{{Partition: 0}, {Partition: 2}}
	for _, owned := range []map[int32]bool{nil, {0: true}, {0: true, 2: false}} {
		batches, positions, err := prepareGroupBatches(rows, owned)
		if !errors.Is(err, errGroupRebalanced) || batches != nil || positions != nil {
			t.Fatal("unowned poll produced partial batches", batches, positions, err)
		}
	}
}
