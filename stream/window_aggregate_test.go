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

func TestWindowedAggregateNullableInitializationInputAndLifetimeRecovery(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		b := stream.NewStreamsBuilder()
		initializations := 0
		var inputNulls, oldNulls []bool
		stream.WindowedAggregate(stream.Stream(b, "in", serde.String, serde.String), "aggregated", window.Tumbling{Size: 10, Grace: 5}, serde.Int64,
			func() (int64, bool) { initializations++; return 10, nullable },
			func(k, v string, inputNull bool, old int64, oldNull bool) (int64, bool) {
				inputNulls = append(inputNulls, inputNull)
				oldNulls = append(oldNulls, oldNull)
				if oldNull {
					old = 1000
				}
				if !inputNull && v == "zero" {
					return 0, false
				}
				size := len(v)
				if inputNull {
					size = 5
				}
				return old + int64(len(k)+size), !inputNull && v == "!delete"
			}).To("out")
		plan := build(t, b)
		backend := memory.New()
		a := task(t, plan, spec(), backend)
		feed(t, a, "in", 0, 1, []byte("a"), nil)
		feed(t, a, "in", 1, 2, []byte("a"), []byte{})
		_ = a.Close()
		restored := task(t, plan, spec(), backend)
		feed(t, restored, "in", 1, 2, []byte("a"), []byte{}) // replay
		feed(t, restored, "in", 2, 1, []byte("a"), []byte("!delete"))
		feed(t, restored, "in", 3, 0, []byte("a"), []byte("zero"))
		feed(t, restored, "in", 4, 3, []byte("a"), nil)
		feed(t, restored, "in", 5, 1000, nil, []byte("ignored"))
		feed(t, restored, "in", 6, 4, []byte("a"), []byte("after"))
		_ = restored.Close()
		out := backend.Outputs(spec())
		want := []int64{16, 17, 0, 0, 6, 12}
		if nullable {
			want[0], want[1] = 1006, 1007
		}
		if len(out) != len(want) || initializations != 2 {
			t.Fatal("initializer ran per input or null/replay filtering changed", len(out), initializations)
		}
		for i, r := range out {
			if i == 2 {
				if r.Value != nil {
					t.Fatal("aggregate deletion did not emit null")
				}
			} else if r.Value == nil || long(t, r.Value) != want[i] {
				t.Fatal("typed aggregate state changed", i, r, want[i])
			}
			if r.Timestamp != []int64{1, 2, 2, 0, 3, 4}[i] {
				t.Fatal("aggregate timestamp lifetime failed", i, r.Timestamp)
			}
		}
		if !reflect.DeepEqual(inputNulls, []bool{true, false, false, false, true, false}) || !reflect.DeepEqual(oldNulls, []bool{nullable, false, false, nullable, false, false}) {
			t.Fatal("nullable input/old state flags conflated", inputNulls, oldNulls)
		}
	}
}

func TestHoppingAggregateFailureRollsBackTypedStateAndInitialization(t *testing.T) {
	b := stream.NewStreamsBuilder()
	fail := true
	boom := errors.New("second aggregate window failed")
	updates := stream.HoppingWindowedAggregate(stream.Stream(b, "in", serde.String, serde.String), "aggregated", window.Hopping{Size: 10, Advance: 5, Grace: 5}, serde.Int64,
		func() (int64, bool) { return 10, false },
		func(_, v string, _ bool, old int64, _ bool) (int64, bool) { return old + int64(len(v)), false })
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
	r := record.Bytes{Topic: "in", Timestamp: 6, Key: []byte("a"), Value: []byte("x")}
	if err := a.Process(ctx, r); !errors.Is(err, boom) || len(backend.Outputs(spec())) != 0 {
		t.Fatal("failed aggregate fanout escaped", err)
	}
	_ = a.Close()
	fail = false
	restored := task(t, plan, spec(), backend)
	defer restored.Close()
	if err := restored.Process(ctx, r); err != nil {
		t.Fatal(err)
	}
	if out := backend.Outputs(spec()); len(out) != 2 || long(t, out[0].Value) != 11 || long(t, out[1].Value) != 11 {
		t.Fatal("aborted typed state survived recovery", out)
	}
}

func TestWindowedAggregateValidatesCallbacksAndCodec(t *testing.T) {
	for _, invalid := range []string{"initializer", "aggregator", "codec"} {
		b := stream.NewStreamsBuilder()
		init := func() (int64, bool) { return 0, false }
		aggregate := func(_, _ string, _ bool, old int64, _ bool) (int64, bool) { return old, false }
		codec := serde.Int64
		switch invalid {
		case "initializer":
			init = nil
		case "aggregator":
			aggregate = nil
		case "codec":
			codec = serde.Codec[int64]{}
		}
		stream.WindowedAggregate(stream.Stream(b, "in", serde.String, serde.String), "aggregated", window.Tumbling{Size: 10}, codec, init, aggregate).To("out")
		if _, err := b.Build(); err == nil {
			t.Fatal("invalid window aggregate admitted", invalid)
		}
	}
}
