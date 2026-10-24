package stream_test

import (
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func TestLeftJoinDistinguishesMissingEmptyAndNullKeys(t *testing.T) {
	b := stream.NewStreamsBuilder()
	table := stream.Table(b, "table", "lookup", serde.String, serde.String)
	source := stream.Stream(b, "in", serde.String, serde.String)
	stream.LeftJoin(source, table, "left", serde.String, func(left, right string, rightNull bool) (string, bool) {
		if left == "delete" {
			return "ignored", true
		}
		if rightNull {
			right = "<missing>"
		}
		return left + ":" + right, false
	}).To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	defer a.Close()
	feed(t, a, "table", 0, 30, []byte{}, []byte{})
	feed(t, a, "table", 1, 31, []byte("a"), []byte{})
	feed(t, a, "in", 0, 10, []byte("a"), []byte("x"))
	feed(t, a, "in", 1, 11, nil, []byte("null-key"))
	feed(t, a, "in", 2, 12, []byte("b"), []byte("x"))
	feed(t, a, "in", 3, 13, []byte("a"), nil)
	feed(t, a, "table", 2, 32, []byte("a"), nil)
	feed(t, a, "in", 4, 14, []byte("a"), []byte("x"))
	feed(t, a, "in", 5, 15, []byte{}, []byte("x"))
	feed(t, a, "in", 6, 16, []byte("b"), []byte("delete"))
	feed(t, a, "in", 7, 17, nil, []byte("delete"))
	out := backend.Outputs(spec())
	want := []string{"x:", "null-key:<missing>", "x:<missing>", "x:<missing>", "x:"}
	if len(out) != 7 {
		t.Fatal("wrong left-triggered output count", len(out))
	}
	for i, value := range want {
		if string(out[i].Value) != value {
			t.Fatalf("left join row %d: %q, expected %q", i, out[i].Value, value)
		}
	}
	if out[0].Timestamp != 10 || out[1].Key != nil || out[4].Key == nil || len(out[4].Key) != 0 || out[5].Value != nil || out[6].Key != nil || out[6].Value != nil {
		t.Fatal("join lost timestamp/null/empty flags", out)
	}
}

func TestNullableInnerJoinForwardsNullResultAndSkipsAbsentInput(t *testing.T) {
	b := stream.NewStreamsBuilder()
	table := stream.Table(b, "table", "lookup", serde.String, serde.String)
	source := stream.Stream(b, "in", serde.String, serde.String)
	stream.JoinNullable(source, table, "inner", serde.String, func(string, string) (string, bool) { return "ignored", true }).To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	defer a.Close()
	feed(t, a, "table", 0, 1, []byte("a"), []byte("A"))
	feed(t, a, "in", 0, 2, []byte("a"), []byte("x"))
	feed(t, a, "in", 1, 3, nil, []byte("x"))
	feed(t, a, "in", 2, 4, []byte("missing"), []byte("x"))
	feed(t, a, "in", 3, 5, []byte("a"), nil)
	out := backend.Outputs(spec())
	if len(out) != 1 || out[0].Value != nil || string(out[0].Key) != "a" || out[0].Timestamp != 2 {
		t.Fatal("nullable inner join changed filtering/emission", out)
	}
}

func TestLeftJoinRestoresTableAndSkipsBothSourceReplays(t *testing.T) {
	b := stream.NewStreamsBuilder()
	table := stream.Table(b, "table", "lookup", serde.String, serde.String)
	stream.LeftJoin(stream.Stream(b, "in", serde.String, serde.String), table, "left", serde.String, func(left, right string, missing bool) (string, bool) {
		if missing {
			return "missing", false
		}
		return left + right, false
	}).To("out")
	plan := build(t, b)
	backend := memory.New()
	a := task(t, plan, spec(), backend)
	feed(t, a, "table", 0, 1, []byte("a"), []byte("A"))
	feed(t, a, "in", 0, 2, []byte("a"), []byte("x"))
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a = task(t, plan, spec(), backend)
	defer a.Close()
	if err := a.ProcessBatch(ctx, []record.Bytes{{Topic: "table", Offset: 0, Timestamp: 1, Key: []byte("a"), Value: nil}, {Topic: "in", Offset: 0, Timestamp: 2, Key: []byte("a"), Value: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	feed(t, a, "in", 1, 3, []byte("a"), []byte("y"))
	feed(t, a, "table", 1, 4, []byte("a"), nil)
	feed(t, a, "in", 2, 5, []byte("a"), []byte("z"))
	out := backend.Outputs(spec())
	if len(out) != 3 || string(out[0].Value) != "xA" || string(out[1].Value) != "yA" || string(out[2].Value) != "missing" {
		t.Fatal("left join recovery/replay failed", out)
	}
}

func TestNullableJoinBuildRejectsNilAndChangedKeys(t *testing.T) {
	for _, mode := range []string{"nil-left", "nil-inner", "changed-key"} {
		b := stream.NewStreamsBuilder()
		table := stream.Table(b, "table", "lookup", serde.String, serde.String)
		source := stream.Stream(b, "in", serde.String, serde.String)
		if mode == "changed-key" {
			source = stream.SelectKey(source, "key", serde.String, func(r record.Record[string, string]) (string, bool) { return r.Value, false })
		}
		switch mode {
		case "nil-left":
			stream.LeftJoin(source, table, "left", serde.String, nil)
		case "nil-inner":
			stream.JoinNullable(source, table, "inner", serde.String, nil)
		default:
			stream.LeftJoin(source, table, "left", serde.String, func(string, string, bool) (string, bool) { return "x", false })
		}
		if _, err := b.Build(); err == nil {
			t.Fatal("invalid join accepted", mode)
		}
	}
}
