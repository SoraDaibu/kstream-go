package stream_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
	"github.com/SoraDaibu/kstream-go/window"
)

var ctx = context.Background()

func build(t *testing.T, b *stream.StreamsBuilder) stream.Topology {
	t.Helper()
	p, e := b.Build()
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func task(t *testing.T, p stream.Topology, s execution.Spec, b execution.Backend) *execution.Task {
	t.Helper()
	v, e := p.NewTask(ctx, s, b)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func feed(t *testing.T, task *execution.Task, topic string, offset, timestamp int64, key, value []byte) {
	t.Helper()
	if e := task.Process(ctx, record.Bytes{Topic: topic, Offset: offset, Timestamp: timestamp, Key: key, Value: value}); e != nil {
		t.Fatal(e)
	}
}
func long(t *testing.T, b []byte) int64 {
	t.Helper()
	v, e := serde.Int64.Decode(b)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func spec() execution.Spec { return execution.Spec{ApplicationID: "test", ID: "0", Partition: 0} }

func TestCountRecoveryTimestampsNullsAndReplay(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	stream.Count(s, "count").ToStream().To("out")
	p := build(t, b)
	backend := memory.New()
	id := spec()
	first := task(t, p, id, backend)
	feed(t, first, "in", 0, 20, []byte("a"), []byte("x"))
	feed(t, first, "in", 1, 10, []byte("a"), []byte("x"))
	feed(t, first, "in", 2, 30, nil, []byte("x"))
	feed(t, first, "in", 3, 40, []byte("a"), nil)
	// Empty keys and values are non-null data, and must not disappear.
	feed(t, first, "in", 4, 50, []byte{}, []byte{})
	restored := task(t, p, id, backend)
	feed(t, restored, "in", 1, 10, []byte("a"), []byte("x")) // committed replay suppressed
	feed(t, restored, "in", 5, 60, []byte("a"), []byte("x"))
	out := backend.Outputs(id)
	if len(out) != 4 {
		t.Fatalf("outputs=%d", len(out))
	}
	got := []int64{}
	for _, r := range out {
		got = append(got, long(t, r.Value))
	}
	if !reflect.DeepEqual(got, []int64{1, 2, 1, 3}) {
		t.Fatal(got)
	}
	if out[1].Timestamp != 20 {
		t.Fatalf("aggregate timestamp regressed: %d", out[1].Timestamp)
	}
	if out[2].Key == nil {
		t.Fatal("empty key became a null")
	}
	if err := first.Process(ctx, record.Bytes{Topic: "in", Offset: 6, Timestamp: 70, Key: []byte("a"), Value: []byte("x")}); !errors.Is(err, execution.ErrFenced) {
		t.Fatalf("stale generation: %v", err)
	}
}

func TestTableDeletesAndStreamTriggeredJoin(t *testing.T) {
	b := stream.NewStreamsBuilder()
	table := stream.Table(b, "table", "lookup", serde.String, serde.String)
	table.ToStream().To("updates")
	s := stream.Stream(b, "in", serde.String, serde.String)
	stream.Join(s, table, "join", serde.String, func(a, b string) string { return a + ":" + b }).To("out")
	p := build(t, b)
	backend := memory.New()
	id := spec()
	a := task(t, p, id, backend)
	feed(t, a, "in", 0, 0, []byte("a"), []byte("before"))
	feed(t, a, "table", 0, 1, []byte("a"), []byte("A"))
	feed(t, a, "table", 1, 2, nil, []byte("ignored"))
	feed(t, a, "in", 1, 3, []byte("a"), []byte("one"))
	feed(t, a, "table", 2, 4, []byte("a"), nil)
	a = task(t, p, id, backend) // tombstone survives changelog recovery
	feed(t, a, "in", 2, 5, []byte("a"), []byte("after"))
	feed(t, a, "table", 3, 6, []byte("a"), []byte{})
	feed(t, a, "in", 3, 7, []byte("a"), []byte("two"))
	out := backend.Outputs(id)
	var joined []string
	updates := 0
	deleted := false
	for _, r := range out {
		if r.Topic == "out" {
			joined = append(joined, string(r.Value))
		} else {
			updates++
			if r.Value == nil {
				deleted = true
			}
		}
	}
	if !reflect.DeepEqual(joined, []string{"one:A", "two:"}) || updates != 3 || !deleted {
		t.Fatalf("joins=%v updates=%d delete=%v", joined, updates, deleted)
	}
}

func TestWindowGraceHalfOpenAndRecovery(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	stream.WindowedCount(s, "windows", window.Tumbling{Size: 10, Grace: 5}).To("out")
	p := build(t, b)
	backend := memory.New()
	id := spec()
	a := task(t, p, id, backend)
	feed(t, a, "in", 0, 9, []byte("a"), []byte("x"))
	feed(t, a, "in", 1, 10, []byte("a"), []byte("x")) // new half-open window
	feed(t, a, "in", 2, 14, []byte("clock"), []byte("x"))
	feed(t, a, "in", 3, 8, []byte("a"), []byte("x")) // before grace closes
	a = task(t, p, id, backend)
	feed(t, a, "in", 4, 15, []byte("clock"), []byte("x")) // closes [0,10)
	feed(t, a, "in", 5, 9, []byte("a"), []byte("x"))      // exactly closed; drop
	out := backend.Outputs(id)
	if len(out) != 5 {
		t.Fatalf("output count %d", len(out))
	}
	key, e := serde.JSON[window.Key[string]]().Decode(out[1].Key)
	if e != nil || key.Start != 10 || key.End != 20 {
		t.Fatalf("window: %+v %v", key, e)
	}
	if long(t, out[3].Value) != 2 || out[3].Timestamp != 9 {
		t.Fatalf("late update: %+v", out[3])
	}
}

func TestWindowClockIsOperatorLocal(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	// Unrelated sources and null keys must not close this window.
	stream.Stream(b, "clock", serde.String, serde.String)
	stream.WindowedCount(s, "w", window.Tumbling{Size: 10, Grace: 5}).To("out")
	p := build(t, b)
	backend := memory.New()
	a := task(t, p, spec(), backend)
	feed(t, a, "clock", 0, 100, []byte("clock"), []byte("x"))
	feed(t, a, "in", 0, 100, nil, nil)
	feed(t, a, "in", 1, 0, []byte("a"), []byte("x"))
	if len(backend.Outputs(spec())) != 1 {
		t.Fatal("unrelated or null input advanced the window clock")
	}
}

func TestProcessorFactoriesOwnStatePerTask(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	s.Process("local", false, func(_ processor.Context, next processor.Forward[string, string]) processor.Handler[string, string] {
		seen := int64(0)
		return func(r record.Record[string, string]) error {
			seen++
			if seen == 1 {
				return next(r)
			}
			return nil
		}
	}).To("out")
	p := build(t, b)
	backend := memory.New()
	one := spec()
	two := one
	two.ID = "other"
	a := task(t, p, one, backend)
	z := task(t, p, two, backend)
	feed(t, a, "in", 0, 0, []byte("a"), []byte("x"))
	feed(t, a, "in", 1, 1, []byte("a"), []byte("x"))
	feed(t, z, "in", 0, 0, []byte("z"), []byte("x"))
	if len(backend.Outputs(one)) != 1 || len(backend.Outputs(two)) != 1 {
		t.Fatal("processor state leaked between tasks")
	}
}

func TestBuildRejectsUnsupportedOrInvalidPlans(t *testing.T) {
	cases := map[string]func(*stream.StreamsBuilder){
		"repartition required": func(b *stream.StreamsBuilder) {
			s := stream.Stream(b, "in", serde.String, serde.String)
			changed := stream.Map(s, "map", serde.String, serde.String, func(r record.Record[string, string]) record.Record[string, string] { return r })
			stream.Count(changed, "count")
		},
		"empty exchange name": func(b *stream.StreamsBuilder) {
			stream.Stream(b, "in", serde.String, serde.String).Repartition("").To("out")
		},
		"duplicate stores": func(b *stream.StreamsBuilder) {
			s := stream.Stream(b, "in", serde.String, serde.String)
			stream.Count(s, "same")
			stream.Count(s, "same")
		},
		"duplicate source": func(b *stream.StreamsBuilder) {
			stream.Stream(b, "in", serde.String, serde.String)
			stream.Stream(b, "in", serde.String, serde.String)
		},
		"invalid window": func(b *stream.StreamsBuilder) {
			stream.WindowedCount(stream.Stream(b, "in", serde.String, serde.String), "w", window.Tumbling{Size: 0})
		},
		"invalid codec": func(b *stream.StreamsBuilder) { stream.Stream(b, "in", serde.Codec[string]{}, serde.String) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			b := stream.NewStreamsBuilder()
			fn(b)
			if _, e := b.Build(); e == nil {
				t.Fatal("invalid plan accepted")
			}
		})
	}
}

func TestBuildFreezesAndPlanIsImmutable(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	s.To("out")
	p := build(t, b)
	before := p.Describe()
	nodes := p.Plan().Nodes()
	nodes[0].Name = "corrupt"
	nodes[1].Parents[0] = "corrupt"
	if p.Describe() != before {
		t.Fatal("plan leaked mutable slices")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("mutation after Build accepted")
		}
	}()
	s.To("later")
}

func TestMapBranchesAndNullFlags(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	s.To("first")
	stream.Map(s, "length", serde.String, serde.Int64, func(r record.Record[string, string]) record.Record[string, int64] {
		return record.Record[string, int64]{Key: r.Key, Value: int64(len(r.Value)), Timestamp: r.Timestamp, NullKey: r.NullKey, NullValue: r.NullValue}
	}).To("second")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	feed(t, a, "in", 0, 1, nil, nil)
	out := backend.Outputs(spec())
	if len(out) != 2 || out[0].Topic != "first" || out[1].Topic != "second" || out[1].Key != nil || out[1].Value != nil {
		t.Fatalf("branch/null semantics: %+v", out)
	}
}

func TestBranchFailureAbortsEarlierOutputAndState(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	stream.Count(s, "count").ToStream().To("first")
	boom := errors.New("processor failed")
	s.Process("failure", false, func(_ processor.Context, _ processor.Forward[string, string]) processor.Handler[string, string] {
		return func(record.Record[string, string]) error { return boom }
	})
	p := build(t, b)
	backend := memory.New()
	a := task(t, p, spec(), backend)
	err := a.Process(ctx, record.Bytes{Topic: "in", Key: []byte("a"), Value: []byte("x")})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if len(backend.Outputs(spec())) != 0 {
		t.Fatal("earlier branch output escaped aborted transaction")
	}
	if _, ok, _ := a.StoreGet("count", "a"); ok {
		t.Fatal("earlier branch state escaped aborted transaction")
	}
}

func TestCountTableJoinUsesStoredAggregateCodec(t *testing.T) {
	b := stream.NewStreamsBuilder()
	counts := stream.Count(stream.Stream(b, "events", serde.String, serde.String), "counts")
	queries := stream.Stream(b, "queries", serde.String, serde.String)
	stream.Join(queries, counts, "lookup-count", serde.Int64, func(_ string, n int64) int64 { return n }).To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	feed(t, a, "events", 0, 0, []byte("a"), []byte("x"))
	feed(t, a, "events", 1, 1, []byte("a"), []byte("x"))
	feed(t, a, "queries", 0, 2, []byte("a"), []byte("query"))
	out := backend.Outputs(spec())
	if len(out) != 1 || long(t, out[0].Value) != 2 {
		t.Fatalf("aggregate lookup output: %+v", out)
	}
}

func TestMultipleBranchesCanWriteTheSameTopic(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	s.To("out")
	s.Filter("branch", func(record.Record[string, string]) bool { return true }).To("out")
	p := build(t, b)
	backend := memory.New()
	a := task(t, p, spec(), backend)
	feed(t, a, "in", 0, 0, []byte("a"), []byte("x"))
	out := backend.Outputs(spec())
	if len(out) != 2 || out[0].Topic != "out" || out[1].Topic != "out" {
		t.Fatalf("shared sink topic output: %+v", out)
	}
}

// Kafka Streams 3.9.1 deliberately differs here from unwindowed count:
// a present key with a null value still contributes to windowed count.
func TestWindowedCountIncludesNullValuesWithPresentKeys(t *testing.T) {
	b := stream.NewStreamsBuilder()
	s := stream.Stream(b, "in", serde.String, serde.String)
	stream.WindowedCount(s, "windows", window.Tumbling{Size: 10, Grace: 5}).To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	feed(t, a, "in", 0, 1, []byte("a"), nil)
	feed(t, a, "in", 1, 2, []byte("a"), []byte("x"))
	feed(t, a, "in", 2, 3, nil, []byte("ignored"))
	out := backend.Outputs(spec())
	if len(out) != 2 || long(t, out[0].Value) != 1 || long(t, out[1].Value) != 2 {
		t.Fatalf("windowed null-value count: %+v", out)
	}
}
