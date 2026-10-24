package stream_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func TestReduceFirstValueDeleteReinitializeTimestampAndReplay(t *testing.T) {
	b := stream.NewStreamsBuilder()
	calls := 0
	s := stream.Stream(b, "in", serde.String, serde.String)
	s.GroupByKey().Reduce("reduced", func(a, v string) (string, bool) {
		calls++
		return a + v, v == "delete"
	}).ToStream().To("out")
	plan := build(t, b)
	backend := memory.New()
	a := task(t, plan, spec(), backend)
	rows := []record.Bytes{
		{Topic: "in", Offset: 0, Timestamp: 100, Key: []byte("a"), Value: []byte{}},
		{Topic: "in", Offset: 1, Timestamp: 20, Key: []byte("a"), Value: []byte("delete")},
		{Topic: "in", Offset: 2, Timestamp: 5, Key: []byte("a"), Value: []byte("new")},
		{Topic: "in", Offset: 3, Timestamp: 1, Key: []byte("a"), Value: []byte("old")},
		{Topic: "in", Offset: 4, Timestamp: 110, Key: nil, Value: []byte("ignored")},
		{Topic: "in", Offset: 5, Timestamp: 120, Key: []byte("a"), Value: nil},
	}
	if err := a.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("reducer called for first value or null input", calls)
	}
	out := backend.Outputs(spec())
	if len(out) != 4 || out[0].Value == nil || len(out[0].Value) != 0 || out[1].Value != nil || string(out[3].Value) != "newold" {
		t.Fatalf("nullable reduce output: %+v", out)
	}
	var timestamps []int64
	for _, r := range out {
		timestamps = append(timestamps, r.Timestamp)
	}
	if !reflect.DeepEqual(timestamps, []int64{100, 100, 5, 5}) {
		t.Fatal("aggregate lifetime clock", timestamps)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a = task(t, plan, spec(), backend)
	defer a.Close()
	if err := a.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	feed(t, a, "in", 6, 2, []byte("a"), []byte("!"))
	out = backend.Outputs(spec())
	if len(out) != 5 || string(out[4].Value) != "newold!" || out[4].Timestamp != 5 {
		t.Fatal("reduce restore/replay failed", out)
	}
}

func TestAggregateNullableInitializerDeletesAndTableLookup(t *testing.T) {
	b := stream.NewStreamsBuilder()
	initialized := 0
	grouped := stream.Stream(b, "in", serde.String, serde.String).GroupByKey()
	agg := stream.Aggregate(grouped, "length", serde.Int64, func() (int64, bool) {
		initialized++
		return 999, true // Null must be passed explicitly, not inferred from zero.
	}, func(k, v string, old int64, oldNull bool) (int64, bool) {
		if oldNull {
			old = 0
		}
		return old + int64(len(k)+len(v)), v == "delete"
	})
	agg.ToStream().To("updates")
	stream.Join(stream.Stream(b, "lookup", serde.String, serde.String), agg, "join", serde.Int64,
		func(_ string, n int64) int64 { return n }).To("joined")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	defer a.Close()
	feed(t, a, "in", 0, 10, []byte("a"), []byte("x"))
	feed(t, a, "lookup", 0, 11, []byte("a"), []byte("?"))
	feed(t, a, "in", 1, 5, []byte("a"), []byte("delete"))
	feed(t, a, "lookup", 1, 12, []byte("a"), []byte("missing"))
	feed(t, a, "in", 2, 1, []byte("a"), []byte{})
	feed(t, a, "lookup", 2, 13, []byte("a"), []byte("?"))
	if initialized != 2 {
		t.Fatal("initializer did not reset after deletion", initialized)
	}
	out := backend.Outputs(spec())
	if len(out) != 5 || out[2].Value != nil || out[2].Timestamp != 10 || out[3].Timestamp != 1 || out[4].Topic != "joined" || long(t, out[4].Value) != 1 {
		t.Fatalf("aggregate/table state mismatch: %+v", out)
	}
}

func TestAggregateCodecFailureAbortsStateAndOutput(t *testing.T) {
	for _, nilBytes := range []bool{false, true} {
		b := stream.NewStreamsBuilder()
		failure := errors.New("aggregate encode failure")
		codec := serde.Codec[int64]{Decode: serde.Int64.Decode, Encode: func(int64) ([]byte, error) {
			if nilBytes {
				return nil, nil
			}
			return nil, failure
		}}
		stream.Aggregate(stream.Stream(b, "in", serde.String, serde.String).GroupByKey(), "bad", codec,
			func() (int64, bool) { return 0, false }, func(string, string, int64, bool) (int64, bool) { return 1, false }).ToStream().To("out")
		backend := memory.New()
		a := task(t, build(t, b), spec(), backend)
		err := a.Process(ctx, record.Bytes{Topic: "in", Key: []byte("a"), Value: []byte("x")})
		if err == nil || (!nilBytes && !errors.Is(err, failure)) || len(backend.Outputs(spec())) != 0 {
			t.Fatal("invalid serialized aggregate committed", err)
		}
		if err = a.Process(ctx, record.Bytes{Topic: "in"}); !errors.Is(err, execution.ErrPoisoned) {
			t.Fatal(err)
		}
		a.Close()
	}
}

func TestGroupingRejectsChangedKeysAndInvalidFunctions(t *testing.T) {
	for _, test := range []string{"changed-key", "nil-reducer", "nil-initializer", "nil-aggregator", "invalid-codec", "empty-store"} {
		t.Run(test, func(t *testing.T) {
			b := stream.NewStreamsBuilder()
			s := stream.Stream(b, "in", serde.String, serde.String)
			if test == "changed-key" {
				s = stream.SelectKey(s, "key", serde.String, func(r record.Record[string, string]) (string, bool) { return r.Value, false })
			}
			g := s.GroupByKey()
			switch test {
			case "changed-key":
				g.Count("counts")
			case "nil-reducer":
				g.Reduce("reduced", nil)
			default:
				initializer := func() (int64, bool) { return 0, false }
				aggregator := func(string, string, int64, bool) (int64, bool) { return 1, false }
				codec, store := serde.Int64, "aggregated"
				if test == "nil-initializer" {
					initializer = nil
				}
				if test == "nil-aggregator" {
					aggregator = nil
				}
				if test == "invalid-codec" {
					codec = serde.Codec[int64]{}
				}
				if test == "empty-store" {
					store = ""
				}
				stream.Aggregate(g, store, codec, initializer, aggregator)
			}
			if _, err := b.Build(); err == nil {
				t.Fatal("invalid grouping accepted")
			}
		})
	}
}
