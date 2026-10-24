package stream_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
	"github.com/SoraDaibu/kstream-go/window"
)

func TestWindowedReduceDeletionRecoveryNullsAndTimestampLifetimes(t *testing.T) {
	b := stream.NewStreamsBuilder()
	calls := 0
	stream.WindowedReduce(stream.Stream(b, "in", serde.String, serde.String), "reduced", window.Tumbling{Size: 10, Grace: 5}, func(old, value string) (string, bool) {
		calls++
		return old + ":" + value, value == "!delete"
	}).To("out")
	plan := build(t, b)
	backend := memory.New()
	first := task(t, plan, spec(), backend)
	if err := first.ProcessBatch(ctx, []record.Bytes{
		{Topic: "in", Offset: 0, Timestamp: 9, Key: []byte("a"), Value: []byte("first")},
		{Topic: "in", Offset: 1, Timestamp: 4, Key: []byte("a"), Value: []byte("second")},
		{Topic: "in", Offset: 2, Timestamp: 3, Key: []byte("a"), Value: []byte("!delete")},
	}); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	restored := task(t, plan, spec(), backend)
	defer restored.Close()
	rows := []record.Bytes{
		{Topic: "in", Offset: 2, Timestamp: 3, Key: []byte("a"), Value: []byte("!delete")}, // replay
		{Topic: "in", Offset: 3, Timestamp: 2, Key: []byte("a"), Value: []byte("!delete")}, // first value bypasses reducer
		{Topic: "in", Offset: 4, Timestamp: 5, Key: []byte("a"), Value: []byte{}},
		{Topic: "in", Offset: 5, Timestamp: 1000, Key: nil, Value: []byte("ignored")},
		{Topic: "in", Offset: 6, Timestamp: 1000, Key: []byte("a"), Value: nil},
		{Topic: "in", Offset: 7, Timestamp: 6, Key: []byte("a"), Value: []byte("next")},
		{Topic: "in", Offset: 8, Timestamp: 15, Key: []byte("clock"), Value: []byte("advance")},
		{Topic: "in", Offset: 9, Timestamp: 9, Key: []byte("a"), Value: []byte("closed")},
		{Topic: "in", Offset: 10, Timestamp: 10, Key: []byte("a"), Value: []byte("new-window")},
	}
	if err := restored.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	out := backend.Outputs(spec())
	if len(out) != 8 || calls != 4 {
		t.Fatal("nulls/closed/replayed records changed reduce state", len(out), calls)
	}
	var values []string
	var timestamps []int64
	for _, r := range out {
		if r.Value == nil {
			values = append(values, "<null>")
		} else {
			values = append(values, string(r.Value))
		}
		timestamps = append(timestamps, r.Timestamp)
	}
	if !reflect.DeepEqual(values, []string{"first", "first:second", "<null>", "!delete", "!delete:", "!delete::next", "advance", "new-window"}) ||
		!reflect.DeepEqual(timestamps, []int64{9, 9, 9, 2, 5, 6, 15, 10}) {
		t.Fatal("reduce initialization/deletion lifetime changed", values, timestamps)
	}
}

func TestHoppingReduceKeepsOpenOverlapAndAscendingWindowOrder(t *testing.T) {
	b := stream.NewStreamsBuilder()
	stream.HoppingWindowedReduce(stream.Stream(b, "in", serde.String, serde.String), "reduced", window.Hopping{Size: 10, Advance: 5, Grace: 5},
		func(old, v string) (string, bool) { return old + ":" + v, false }).To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	defer a.Close()
	feed(t, a, "in", 0, 9, []byte("a"), []byte("x"))
	feed(t, a, "in", 1, 15, []byte("clock"), []byte("x"))
	feed(t, a, "in", 2, 9, []byte("a"), []byte("y"))
	feed(t, a, "in", 3, 20, []byte("clock"), []byte("x"))
	feed(t, a, "in", 4, 9, []byte("a"), []byte("closed"))
	var starts []int64
	var values []string
	for _, r := range backend.Outputs(spec()) {
		key, err := serde.JSON[window.Key[string]]().Decode(r.Key)
		if err != nil {
			t.Fatal(err)
		}
		if key.Key == "a" {
			starts = append(starts, key.Start)
			values = append(values, string(r.Value))
		}
	}
	if !reflect.DeepEqual(starts, []int64{0, 5, 5}) || !reflect.DeepEqual(values, []string{"x", "x", "x:y"}) {
		t.Fatal("window closure dropped a live overlap or reordered fanout", starts, values)
	}
}

func TestHoppingReduceFailureDiscardsDerivedIndexAndAllFanout(t *testing.T) {
	b := stream.NewStreamsBuilder()
	fail := true
	boom := errors.New("second window failed")
	updates := stream.HoppingWindowedReduce(stream.Stream(b, "in", serde.String, serde.Int64), "reduced", window.Hopping{Size: 10, Advance: 5, Grace: 5},
		func(old, v int64) (int64, bool) { return old + v, false })
	updates.To("out")
	updates.Foreach("reject", func(r record.Record[window.Key[string], int64]) error {
		if fail && r.Key.Start == 5 {
			return boom
		}
		return nil
	})
	plan := build(t, b)
	backend := memory.New()
	a := task(t, plan, spec(), backend)
	zero, _ := serde.Int64.Encode(0)
	r := record.Bytes{Topic: "in", Timestamp: 6, Key: []byte("a"), Value: zero}
	if err := a.Process(ctx, r); !errors.Is(err, boom) || len(backend.Outputs(spec())) != 0 {
		t.Fatal("failed window fanout escaped", err)
	}
	_ = a.Close()
	fail = false
	restored := task(t, plan, spec(), backend)
	defer restored.Close()
	if err := restored.Process(ctx, r); err != nil {
		t.Fatal(err)
	}
	out := backend.Outputs(spec())
	if len(out) != 2 || out[0].Value == nil || out[1].Value == nil || long(t, out[0].Value) != 0 || long(t, out[1].Value) != 0 {
		t.Fatal("zero aggregate became null or aborted fanout survived", out)
	}
}

func TestWindowedReduceRejectsInvalidSetup(t *testing.T) {
	for _, kind := range []string{"reducer", "window", "store", "partition"} {
		t.Run(kind, func(t *testing.T) {
			b := stream.NewStreamsBuilder()
			source := stream.Stream(b, "in", serde.String, serde.String)
			store := "reduced"
			w := window.Hopping{Size: 10, Advance: 5}
			reduce := func(a, v string) (string, bool) { return a + v, false }
			switch kind {
			case "reducer":
				reduce = nil
			case "window":
				w.Advance = 11
			case "store":
				store = ""
			case "partition":
				source = stream.Map(source, "change", serde.String, serde.String, func(r record.Record[string, string]) record.Record[string, string] { return r })
			}
			stream.HoppingWindowedReduce(source, store, w, reduce).To("out")
			if _, err := b.Build(); err == nil {
				t.Fatal("invalid windowed reduce admitted")
			}
		})
	}
}
