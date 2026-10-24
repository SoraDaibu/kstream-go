package stream_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func TestProcessingClocksSeparateComponentsMergeSourcesAndRestore(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		b := stream.NewStreamsBuilder()
		var times []int64
		var metadata []record.Metadata
		var setupTimes []int64
		left := stream.Stream(b, "left", serde.String, serde.String)
		right := stream.Stream(b, "right", serde.String, serde.String)
		stream.Merge(left, right, "merged").Filter("drop", func(r record.Record[string, string]) bool { return r.Value != "drop" }).Process("clock", false,
			func(c processor.Context, next processor.Forward[string, string]) processor.Handler[string, string] {
				setupTimes = append(setupTimes, c.StreamTime())
				return func(r record.Record[string, string]) error {
					times = append(times, c.StreamTime())
					m, ok := c.RecordMetadata()
					if !ok {
						return errors.New("scoped context lost source metadata")
					}
					metadata = append(metadata, m)
					c.StorePut("custom", "key", []byte(r.Value))
					if r.Value == "fail" {
						return errors.New("failed callback")
					}
					return next(r)
				}
			}).To("out")
		// A disconnected source must never advance the merged stage's clock.
		stream.Stream(b, "other", serde.String, serde.String).To("other-out")
		plan, backend := build(t, b), memory.New()
		var storage execution.Backend = backend
		var group *execution.Group
		if grouped {
			var err error
			group, err = execution.NewGroup(backend)
			if err != nil {
				t.Fatal(err)
			}
			storage = group
		}
		process := func(a *execution.Task, rows ...record.Bytes) error {
			if group != nil {
				return group.Process(ctx, []execution.TaskBatch{{Task: a, Records: rows}})
			}
			return a.ProcessBatch(ctx, rows)
		}
		a := task(t, plan, spec(), storage)
		row := func(topic string, offset, stamp int64, value string) record.Bytes {
			return record.Bytes{Topic: topic, Offset: offset, Timestamp: stamp, Key: []byte("k"), Value: []byte(value)}
		}
		if err := process(a, row("left", 0, 9, "first"), row("other", 0, 1000, "unrelated"), row("right", 0, 50, "drop"), row("left", 1, 10, "second")); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(times, []int64{9, 50}) || !reflect.DeepEqual(setupTimes, []int64{-1}) || a.StreamTime() != 1000 {
			t.Fatal("processing clock did not reflect accepted connected source prefix", times, setupTimes, a.StreamTime())
		}
		_ = a.Close()
		a = task(t, plan, spec(), storage)
		if !reflect.DeepEqual(setupTimes, []int64{-1, 50}) {
			t.Fatal("clock was not restored before supplier setup", setupTimes)
		}
		if err := process(a, row("right", 0, 900, "replay"), row("left", 2, 11, "third")); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(times, []int64{9, 50, 50}) || metadata[2] != (record.Metadata{Topic: "left", Offset: 2}) {
			t.Fatal("replay advanced clock or lost source identity", times, metadata)
		}
		if err := process(a, row("left", 3, 200, "fail")); err == nil {
			t.Fatal("failed callback committed")
		}
		_ = a.Close()
		a = task(t, plan, spec(), storage)
		defer a.Close()
		if setupTimes[2] != 50 {
			t.Fatal("failed callback leaked staged clock", setupTimes)
		}
		if err := process(a, row("left", 3, 12, "retry")); err != nil {
			t.Fatal(err)
		}
		if times[len(times)-1] != 50 {
			t.Fatal("aborted progress survived restore", times)
		}
		value, ok, err := a.StoreGet("custom", "key")
		if err != nil || !ok || string(value) != "retry" {
			t.Fatal("scoped context lost state delegation", string(value), ok, err)
		}
	}
}

func TestTypedProcessingClockAfterRepartitionRecoversIndependently(t *testing.T) {
	b := stream.NewStreamsBuilder()
	var observed []int64
	s := stream.Stream(b, "in", serde.String, serde.String).Repartition("exchange")
	stream.Process(s, "typed-clock", serde.String, serde.Int64,
		func(c processor.Context, next processor.Forward[string, int64]) processor.Handler[string, string] {
			return func(r record.Record[string, string]) error {
				observed = append(observed, c.StreamTime())
				return next(record.Record[string, int64]{Key: r.Key, Value: c.StreamTime(), Timestamp: r.Timestamp, NullKey: r.NullKey})
			}
		}).To("out")
	plan, backend := build(t, b), memory.New()
	a := task(t, plan, spec(), backend)
	if err := a.ProcessBatch(ctx, []record.Bytes{
		{Topic: "in", Offset: 0, Timestamp: 9, Key: []byte("k"), Value: []byte("v")},
		{Topic: "in", Offset: 1, Timestamp: 100, Key: []byte("k"), Value: []byte("v")},
	}); err != nil {
		t.Fatal(err)
	}
	input := backend.Outputs(spec())[0]
	input.Offset = 0
	if err := a.Process(ctx, input); err != nil {
		t.Fatal(err)
	}
	_ = a.Close()
	a = task(t, plan, spec(), backend)
	defer a.Close()
	input.Offset, input.Timestamp = 1, 3
	if err := a.Process(ctx, input); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(observed, []int64{9, 9}) {
		t.Fatal("upstream clock or old timestamp contaminated downstream restore", observed)
	}
}
