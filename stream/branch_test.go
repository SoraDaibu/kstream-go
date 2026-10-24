package stream_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func TestBranchFirstMatchPreservesRecordsAndPredicateOwnership(t *testing.T) {
	b := stream.NewStreamsBuilder()
	var calls []int
	predicates := []func(record.Record[string, string]) bool{
		func(r record.Record[string, string]) bool {
			calls = append(calls, 0)
			return !r.NullValue && r.Value == ""
		},
		func(r record.Record[string, string]) bool { calls = append(calls, 1); return r.NullKey || r.Key == "a" },
		func(record.Record[string, string]) bool { calls = append(calls, 2); return true },
	}
	branches := stream.Stream(b, "in", serde.String, serde.String).Branch("routes", predicates...)
	// Mutating the caller's variadic slice must not change a built topology.
	predicates[0] = func(record.Record[string, string]) bool { t.Fatal("borrowed predicate slice"); return false }
	for i, topic := range []string{"empty", "a-or-null", "other"} {
		branches[i].To(topic)
	}
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	defer a.Close()
	rows := []record.Bytes{
		{Topic: "in", Offset: 0, Timestamp: 9, Key: []byte("a"), Value: []byte{}},
		{Topic: "in", Offset: 1, Timestamp: 3, Key: nil, Value: nil},
		{Topic: "in", Offset: 2, Timestamp: 1, Key: []byte{}, Value: []byte("x")},
	}
	if err := a.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	out := backend.Outputs(spec())
	if len(out) != 3 || out[0].Topic != "empty" || out[1].Topic != "a-or-null" || out[2].Topic != "other" {
		t.Fatal("branch duplicated, lost or misrouted an input", out)
	}
	for i, r := range out {
		if !reflect.DeepEqual(r.Key, rows[i].Key) || !reflect.DeepEqual(r.Value, rows[i].Value) || r.Timestamp != rows[i].Timestamp || r.Partition != rows[i].Partition {
			t.Fatal("branch changed record identity/nulls", i, r)
		}
	}
	if !reflect.DeepEqual(calls, []int{0, 0, 1, 0, 1, 2}) {
		t.Fatal("predicates ran after a first match or in the wrong order", calls)
	}
}

func TestBranchUnmatchedRecordsCommitOffsetsAndRetainKeyLocalState(t *testing.T) {
	b := stream.NewStreamsBuilder()
	branches := stream.Stream(b, "in", serde.String, serde.String).Branch("routes",
		func(r record.Record[string, string]) bool { return r.Value == "yes" && !r.NullValue })
	stream.Count(branches[0], "counts").ToStream().To("out")
	plan := build(t, b)
	backend := memory.New()
	first := task(t, plan, spec(), backend)
	feed(t, first, "in", 0, 1, []byte("a"), []byte("yes"))
	feed(t, first, "in", 1, 2, []byte("a"), []byte("no"))
	_ = first.Close()
	restored := task(t, plan, spec(), backend)
	defer restored.Close()
	feed(t, restored, "in", 1, 3, []byte("a"), []byte("yes")) // committed unmatched offset
	feed(t, restored, "in", 2, 4, []byte("a"), []byte("yes"))
	out := backend.Outputs(spec())
	if len(out) != 2 || long(t, out[0].Value) != 1 || long(t, out[1].Value) != 2 {
		t.Fatal("branch changed key locality, recovery or unmatched offset progress", out)
	}
}

func TestBranchSelectedFailureAbortsWholeInputBatch(t *testing.T) {
	b := stream.NewStreamsBuilder()
	branches := stream.Stream(b, "in", serde.String, serde.String).Branch("routes",
		func(r record.Record[string, string]) bool { return r.Key == "a" },
		func(record.Record[string, string]) bool { return true })
	stream.Count(branches[0], "counts").ToStream().To("out")
	boom := errors.New("branch sink failed")
	branches[1].Foreach("reject", func(record.Record[string, string]) error { return boom })
	plan := build(t, b)
	backend := memory.New()
	a := task(t, plan, spec(), backend)
	err := a.ProcessBatch(ctx, []record.Bytes{
		{Topic: "in", Offset: 0, Key: []byte("a"), Value: []byte("x")},
		{Topic: "in", Offset: 1, Key: []byte("b"), Value: []byte("x")},
	})
	if !errors.Is(err, boom) || len(backend.Outputs(spec())) != 0 {
		t.Fatal("a different branch escaped batch rollback", err)
	}
	_ = a.Close()
	restored := task(t, plan, spec(), backend)
	defer restored.Close()
	feed(t, restored, "in", 0, 1, []byte("a"), []byte("x"))
	if out := backend.Outputs(spec()); len(out) != 1 || long(t, out[0].Value) != 1 {
		t.Fatal("failed branch state/offset was committed", out)
	}
}

func TestBranchRequiresPredicates(t *testing.T) {
	for _, checks := range [][]func(record.Record[string, string]) bool{nil, {nil}} {
		b := stream.NewStreamsBuilder()
		stream.Stream(b, "in", serde.String, serde.String).Branch("routes", checks...)
		if _, err := b.Build(); err == nil {
			t.Fatal("invalid branch predicates admitted")
		}
	}
}
