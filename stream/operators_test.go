package stream_test

import (
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func TestMapValuesNullsAndKeyLocalGrouping(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	mapped := stream.MapValues(s, "values", serde.String, func(v string, null bool) (string, bool) {
		if null {
			return "revived", false
		}
		return v + "!", false
	})
	mapped.To("mapped")
	stream.Count(mapped, "counts").ToStream().To("counted")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	feed(t, a, "in", 0, 7, []byte("a"), nil)
	feed(t, a, "in", 1, 8, []byte("a"), []byte{})
	out := backend.Outputs(spec())
	if len(out) != 4 || string(out[0].Value) != "revived" || string(out[2].Value) != "!" || long(t, out[3].Value) != 2 {
		t.Fatalf("mapValues output: %+v", out)
	}
	if string(out[0].Key) != "a" || out[0].Timestamp != 7 {
		t.Fatal("mapValues changed record identity")
	}
}

func TestMapValuesCanProduceTombstones(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	stream.MapValues(s, "delete", serde.String, func(string, bool) (string, bool) { return "", true }).To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	feed(t, a, "in", 0, 1, []byte("a"), []byte("x"))
	out := backend.Outputs(spec())
	if len(out) != 1 || out[0].Value != nil {
		t.Fatal("mapped null became an empty value")
	}
}

func TestMergeCompilesSharedOperatorOncePerTask(t *testing.T) {
	b := stream.NewStreamsBuilder()
	left := stream.Stream(b, "left", serde.String, serde.String)
	right := stream.Stream(b, "right", serde.String, serde.String)
	factories := 0
	stream.Merge(left, right, "merged").Process("counter", false, func(_ processor.Context, next processor.Forward[string, string]) processor.Handler[string, string] {
		factories++
		seen := 0
		return func(r record.Record[string, string]) error { seen++; r.Value = strconv.Itoa(seen); return next(r) }
	}).To("out")
	plan := build(t, b)
	backend := memory.New()
	a := task(t, plan, spec(), backend)
	feed(t, a, "left", 0, 1, []byte("a"), []byte("x"))
	feed(t, a, "right", 0, 2, []byte("b"), []byte("x"))
	feed(t, a, "left", 1, 3, []byte("c"), []byte("x"))
	var values []string
	for _, r := range backend.Outputs(spec()) {
		values = append(values, string(r.Value))
	}
	if factories != 1 || !reflect.DeepEqual(values, []string{"1", "2", "3"}) {
		t.Fatalf("shared factory=%d output=%v", factories, values)
	}
	id := spec()
	id.ID = "other"
	other := task(t, plan, id, backend)
	feed(t, other, "right", 0, 1, []byte("a"), []byte("x"))
	if factories != 2 || string(backend.Outputs(id)[0].Value) != "1" {
		t.Fatal("compiled graph leaked between tasks")
	}
}

func TestFlatMapForwardsNullsAndAbortsOnError(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(strconv.FormatBool(fail), func(t *testing.T) {
			b := stream.NewStreamsBuilder()
			s := stream.Stream(b, "in", serde.String, serde.String)
			boom := errors.New("mapper failed")
			stream.FlatMap(s, "twice", serde.String, serde.String, func(r record.Record[string, string], next processor.Forward[string, string]) error {
				if err := next(r); err != nil {
					return err
				}
				if fail {
					return boom
				}
				return next(r)
			}).To("out")
			backend := memory.New()
			a := task(t, build(t, b), spec(), backend)
			err := a.Process(ctx, record.Bytes{Topic: "in", Timestamp: 1})
			if fail {
				if !errors.Is(err, boom) || len(backend.Outputs(spec())) != 0 {
					t.Fatal("failed mapper escaped", err)
				}
			} else {
				out := backend.Outputs(spec())
				if err != nil || len(out) != 2 || out[0].Key != nil || out[0].Value != nil || out[1].Timestamp != 1 {
					t.Fatalf("flatMap: %+v %v", out, err)
				}
			}
		})
	}
}

func TestFilterNotPeekAndForeach(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	var observed []string
	s.FilterNot("skip-empty", func(r record.Record[string, string]) bool { return r.Value == "" }).Peek("observe", func(r record.Record[string, string]) error { observed = append(observed, "peek:"+r.Value); return nil }).Foreach("terminal", func(r record.Record[string, string]) error {
		observed = append(observed, "foreach:"+r.Value)
		return nil
	})
	a := task(t, build(t, b), spec(), memory.New())
	feed(t, a, "in", 0, 0, []byte("a"), []byte{})
	feed(t, a, "in", 1, 1, []byte("a"), []byte("value"))
	if !reflect.DeepEqual(observed, []string{"peek:value", "foreach:value"}) {
		t.Fatal(observed)
	}
}

func TestSelectKeyAndKeyChangeBoundary(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	changed := stream.SelectKey(s, "key", serde.String, func(r record.Record[string, string]) (string, bool) { return r.Value, false })
	changed.To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	feed(t, a, "in", 0, 5, nil, []byte("new"))
	out := backend.Outputs(spec())
	if len(out) != 1 || string(out[0].Key) != "new" || out[0].Timestamp != 5 {
		t.Fatalf("selectKey: %+v", out)
	}
	b = stream.NewStreamsBuilder()
	s = stream.Stream(b, "in", serde.String, serde.String)
	changed = stream.SelectKey(s, "key", serde.String, func(r record.Record[string, string]) (string, bool) { return r.Value, false })
	stream.Count(changed, "counts")
	if _, err := b.Build(); err == nil {
		t.Fatal("selectKey bypassed repartition boundary")
	}
}

func TestFlatMapValuesPreservesIdentityAndRejectsRetainedYield(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	var retained func(string, bool) error
	mapped := stream.FlatMapValues(s, "values", serde.String, func(v string, null bool, yield func(string, bool) error) error {
		retained = yield
		if err := yield(v, null); err != nil {
			return err
		}
		return yield("second", false)
	})
	mapped.To("values")
	stream.Count(mapped, "counts").ToStream().To("counts-out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	feed(t, a, "in", 0, 7, []byte("a"), nil)
	out := backend.Outputs(spec())
	if len(out) != 3 || out[0].Value != nil || string(out[1].Value) != "second" || long(t, out[2].Value) != 1 {
		t.Fatalf("flatMapValues: %+v", out)
	}
	if string(out[1].Key) != "a" || out[1].Timestamp != 7 {
		t.Fatal("flatMapValues changed identity")
	}
	if err := retained("late", false); err == nil {
		t.Fatal("retained yield accepted outside transaction")
	}
}

func TestFlatMapValuesCannotHideDownstreamErrors(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	boom := errors.New("downstream")
	stream.FlatMapValues(s, "values", serde.String, func(v string, null bool, yield func(string, bool) error) error { _ = yield(v, null); return nil }).Process("fail", false, func(_ processor.Context, _ processor.Forward[string, string]) processor.Handler[string, string] {
		return func(record.Record[string, string]) error { return boom }
	})
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	if err := a.Process(ctx, record.Bytes{Topic: "in", Key: []byte("a"), Value: []byte("x")}); !errors.Is(err, boom) {
		t.Fatal(err)
	}
}
