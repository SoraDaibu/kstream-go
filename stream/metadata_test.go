package stream_test

import (
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func TestProcessorMetadataTracksOriginalInputThroughFanoutAndReplay(t *testing.T) {
	b := stream.NewStreamsBuilder()
	left := stream.Stream(b, "left", serde.String, serde.String)
	right := stream.Stream(b, "right", serde.String, serde.String)
	merged := stream.Merge(left, right, "merged")
	fanout := stream.FlatMapValues(merged, "twice", serde.String, func(v string, null bool, yield func(string, bool) error) error {
		if err := yield(v, null); err != nil {
			return err
		}
		return yield(v, null)
	})
	var seen []record.Metadata
	factories := 0
	fanout.Process("metadata", false, func(ctx processor.Context, next processor.Forward[string, string]) processor.Handler[string, string] {
		factories++
		if meta, present := ctx.RecordMetadata(); present || meta != (record.Metadata{}) {
			t.Fatal("source metadata present during factory initialization", meta)
		}
		return func(r record.Record[string, string]) error {
			meta, present := ctx.RecordMetadata()
			if !present {
				t.Fatal("missing source metadata during forwarding")
			}
			seen = append(seen, meta)
			return next(r)
		}
	}).To("out")
	backend := memory.New()
	s := execution.Spec{ApplicationID: "metadata", ID: "0", Partition: 2}
	a := task(t, build(t, b), s, backend)
	defer a.Close()
	rows := []record.Bytes{
		{Topic: "left", Partition: 2, Offset: 4, Timestamp: 100, Key: []byte("a"), Value: []byte("left")},
		{Topic: "right", Partition: 2, Offset: 0, Timestamp: 20, Key: nil, Value: nil},
		{Topic: "left", Partition: 2, Offset: 4, Timestamp: 101, Key: []byte("duplicate"), Value: []byte("ignored")},
		{Topic: "left", Partition: 2, Offset: 5, Timestamp: 0, Key: []byte{}, Value: []byte{}},
	}
	if err := a.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := a.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	want := []record.Metadata{{Topic: "left", Partition: 2, Offset: 4}, {Topic: "left", Partition: 2, Offset: 4}, {Topic: "right", Partition: 2, Offset: 0}, {Topic: "right", Partition: 2, Offset: 0}, {Topic: "left", Partition: 2, Offset: 5}, {Topic: "left", Partition: 2, Offset: 5}}
	if factories != 1 || !reflect.DeepEqual(seen, want) || len(backend.Outputs(s)) != 6 {
		t.Fatal("metadata changed source identity or replayed input", factories, seen)
	}
	if meta, present := a.RecordMetadata(); present || meta != (record.Metadata{}) {
		t.Fatal("metadata escaped callback scope", meta)
	}
}
