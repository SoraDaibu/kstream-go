package stream_test

import (
	"errors"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func TestTableTransformsDeleteWithoutCallingUserCode(t *testing.T) {
	b := stream.NewStreamsBuilder()
	table := stream.Table(b, "table", "source", serde.String, serde.String)
	filteredCalls, mappedCalls := 0, 0
	filtered := table.Filter("filtered", func(k, v string) bool {
		filteredCalls++
		return v != "reject"
	})
	mapped := stream.MapTableValues(filtered, "mapped", serde.Int64, func(k, v string) (int64, bool) {
		mappedCalls++
		return int64(len(k) + len(v)), v == "delete"
	})
	mapped.ToStream().To("updates")
	source := stream.Stream(b, "in", serde.String, serde.String)
	stream.LeftJoin(source, mapped, "enrich", serde.String, func(_ string, right int64, missing bool) (string, bool) {
		if missing {
			return "missing", false
		}
		if right == 0 {
			return "zero", false
		}
		return "present", false
	}).To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	defer a.Close()
	feed(t, a, "table", 0, 10, []byte("a"), []byte("x"))
	feed(t, a, "in", 0, 11, []byte("a"), []byte("x"))
	feed(t, a, "table", 1, 2, []byte("a"), []byte("reject"))
	feed(t, a, "in", 1, 12, []byte("a"), []byte("x"))
	feed(t, a, "table", 2, 3, []byte("a"), nil)
	feed(t, a, "table", 3, 4, nil, []byte("ignored"))
	feed(t, a, "table", 4, 5, []byte{}, []byte{})
	feed(t, a, "in", 2, 13, []byte{}, []byte("x"))
	feed(t, a, "table", 5, 6, []byte("a"), []byte("delete"))
	feed(t, a, "in", 3, 14, []byte("a"), []byte("x"))
	if filteredCalls != 4 || mappedCalls != 3 {
		t.Fatal("table tombstone invoked user function", filteredCalls, mappedCalls)
	}
	var lookups, updates []record.Bytes
	for _, r := range backend.Outputs(spec()) {
		if r.Topic == "out" {
			lookups = append(lookups, r)
		} else {
			updates = append(updates, r)
		}
	}
	if len(lookups) != 4 || string(lookups[0].Value) != "present" || string(lookups[1].Value) != "missing" || string(lookups[2].Value) != "zero" || string(lookups[3].Value) != "missing" {
		t.Fatal("derived table retained stale/missing/zero state", lookups)
	}
	if len(updates) != 4 || updates[1].Value != nil || updates[1].Timestamp != 2 || long(t, updates[2].Value) != 0 || updates[3].Value != nil {
		t.Fatal("derived tombstone/zero/timestamp output", updates)
	}
}

func TestMaterializedTableFilterSuppressesRepeatedAbsentDeletes(t *testing.T) {
	b := stream.NewStreamsBuilder()
	stream.Table(b, "table", "source", serde.String, serde.String).Filter("filtered", func(_, v string) bool { return v != "" }).ToStream().To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	defer a.Close()
	for i, value := range [][]byte{nil, {}, {}, nil, []byte("present"), {}, nil} {
		feed(t, a, "table", int64(i), int64(i), []byte("key"), value)
	}
	out := backend.Outputs(spec())
	if len(out) != 2 || string(out[0].Value) != "present" || out[0].Timestamp != 4 || out[1].Value != nil || out[1].Timestamp != 5 {
		t.Fatal("filter forwarded absent-row delete or dropped actual delete", out)
	}
}

func TestTableTransformCodecFailureRestoresSourceAndDerivedTogether(t *testing.T) {
	b := stream.NewStreamsBuilder()
	wanted := errors.New("table encoding rejected")
	codec := serde.Codec[string]{Encode: func(v string) ([]byte, error) {
		if v == "bad" {
			return nil, wanted
		}
		return []byte(v), nil
	}, Decode: serde.String.Decode}
	table := stream.Table(b, "table", "source", serde.String, serde.String)
	stream.MapTableValues(table, "derived", codec, func(_, v string) (string, bool) { return v, false }).ToStream().To("out")
	plan := build(t, b)
	backend := memory.New()
	a := task(t, plan, spec(), backend)
	feed(t, a, "table", 0, 1, []byte("a"), []byte("good"))
	rows := []record.Bytes{{Topic: "table", Offset: 1, Timestamp: 2, Key: []byte("a"), Value: []byte("changed")}, {Topic: "table", Offset: 2, Timestamp: 3, Key: []byte("b"), Value: []byte("bad")}}
	if err := a.ProcessBatch(ctx, rows); !errors.Is(err, wanted) {
		t.Fatal(err)
	}
	if err := a.ProcessBatch(ctx, rows); !errors.Is(err, execution.ErrPoisoned) {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a = task(t, plan, spec(), backend)
	defer a.Close()
	for _, store := range []string{"source", "derived"} {
		v, ok, err := a.StoreGet(store, "a")
		if err != nil || !ok || string(v) != "good" {
			t.Fatal("partial table transaction restored", store, string(v), ok, err)
		}
		if _, ok, err = a.StoreGet(store, "b"); err != nil || ok {
			t.Fatal("aborted table row restored", store, err)
		}
	}
	feed(t, a, "table", 0, 1, []byte("a"), []byte("replayed"))
	feed(t, a, "table", 1, 4, []byte("a"), nil)
	out := backend.Outputs(spec())
	if len(out) != 2 || string(out[0].Value) != "good" || out[1].Value != nil {
		t.Fatal("table replay/restore changed updates", out)
	}
}

func TestTableTransformBuildRejectsInvalidDefinitions(t *testing.T) {
	for _, mode := range []string{"nil-filter", "nil-filter-not", "nil-mapper", "duplicate-store", "invalid-codec"} {
		b := stream.NewStreamsBuilder()
		table := stream.Table(b, "table", "source", serde.String, serde.String)
		switch mode {
		case "nil-filter":
			table.Filter("filtered", nil)
		case "nil-filter-not":
			table.FilterNot("filtered", nil)
		case "nil-mapper":
			stream.MapTableValues(table, "mapped", serde.String, nil)
		case "duplicate-store":
			table.Filter("source", func(string, string) bool { return true })
		case "invalid-codec":
			stream.MapTableValues(table, "mapped", serde.Codec[string]{}, func(_, v string) (string, bool) { return v, false })
		}
		if _, err := b.Build(); err == nil {
			t.Fatal("invalid table transform accepted", mode)
		}
	}
}
