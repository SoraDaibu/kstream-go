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

func TestRepartitionRequiresCommittedTransportBeforeCrossPartitionGrouping(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		b := stream.NewStreamsBuilder()
		changed := stream.SelectKey(stream.Stream(b, "in", serde.String, serde.String), "key", serde.String,
			func(r record.Record[string, string]) (string, bool) { return r.Value, r.NullValue })
		stream.Count(changed.Repartition("by-value"), "counts").ToStream().To("out")
		plan := build(t, b)
		backend := memory.New()
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
		process := func(a *execution.Task, rows ...record.Bytes) {
			t.Helper()
			var err error
			if group == nil {
				err = a.ProcessBatch(ctx, rows)
			} else {
				err = group.Process(ctx, []execution.TaskBatch{{Task: a, Records: rows}})
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		upID, downID := spec(), spec()
		downID.Partition = 1
		up := task(t, plan, upID, storage)
		defer up.Close()
		down := task(t, plan, downID, storage)
		original := record.Bytes{Topic: "in", Offset: 0, Timestamp: 9, Key: []byte("original"), Value: []byte("bucket")}
		process(up, original)
		exchange := backend.Outputs(upID)
		if len(exchange) != 1 || exchange[0].Topic != "test-by-value-repartition" || len(backend.Outputs(downID)) != 0 {
			t.Fatal("exchange bypassed committed transport", exchange)
		}
		// The model transport selects partition 1 and assigns its consumed offset.
		// Real Kafka hashing is independently covered by adapter/broker tests.
		input := exchange[0]
		input.Partition, input.Offset = 1, 0
		process(down, input)
		_ = down.Close()
		down = task(t, plan, downID, storage)
		defer down.Close()
		process(up, original) // upstream replay must not emit another exchange.
		process(down, input)  // committed downstream replay must not update state.
		original.Offset, original.Key = 1, []byte("other-original")
		process(up, original)
		input = backend.Outputs(upID)[1]
		input.Partition, input.Offset = 1, 1
		process(down, input)
		out := backend.Outputs(downID)
		if len(backend.Outputs(upID)) != 2 || len(out) != 2 || long(t, out[0].Value) != 1 || long(t, out[1].Value) != 2 || out[1].Partition != 1 || string(out[1].Key) != "bucket" {
			t.Fatal("cross-partition state/recovery or replay failed", out)
		}
	}
}

func TestRepartitionCoalescedTaskClockAndWindowClockBoundary(t *testing.T) {
	b := stream.NewStreamsBuilder()
	var observed []int64
	s := stream.Stream(b, "in", serde.String, serde.String).Repartition("exchange").Process("clock", false,
		func(c processor.Context, next processor.Forward[string, string]) processor.Handler[string, string] {
			return func(r record.Record[string, string]) error {
				observed = append(observed, c.StreamTime())
				return next(r)
			}
		})
	stream.WindowedCount(s, "windows", window.Tumbling{Size: 10}).To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	defer a.Close()
	if err := a.ProcessBatch(ctx, []record.Bytes{
		{Topic: "in", Offset: 0, Timestamp: 9, Key: []byte("a"), Value: []byte("x")},
		{Topic: "in", Offset: 1, Timestamp: 100, Key: []byte("a"), Value: []byte("x")},
	}); err != nil {
		t.Fatal(err)
	}
	input := backend.Outputs(spec())[0]
	input.Offset = 0
	if err := a.Process(ctx, input); err != nil {
		t.Fatal(err)
	}
	out := backend.Outputs(spec())
	// The current runtime coalesces stages into a partition-wide task clock.
	// Window operators retain their own input clock and must still accept t=9.
	if !reflect.DeepEqual(observed, []int64{100}) || len(out) != 3 || out[2].Topic != "out" || out[2].Timestamp != 9 {
		t.Fatal("documented task/window clock boundary changed", observed, out)
	}
}

func TestRepartitionPreservesNullEmptyValuesAndTimestamps(t *testing.T) {
	b := stream.NewStreamsBuilder()
	stream.Stream(b, "in", serde.String, serde.String).Repartition("exchange").To("out")
	plan := build(t, b)
	backend := memory.New()
	upID, downID := spec(), spec()
	downID.Partition = 1
	up, down := task(t, plan, upID, backend), task(t, plan, downID, backend)
	defer up.Close()
	defer down.Close()
	rows := []record.Bytes{
		{Topic: "in", Offset: 0, Timestamp: 9},
		{Topic: "in", Offset: 1, Timestamp: 2, Key: []byte{}, Value: []byte{}},
		{Topic: "in", Offset: 2, Timestamp: 1, Key: []byte("a"), Value: nil},
	}
	if err := up.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	exchanges := backend.Outputs(upID)
	for i := range exchanges {
		exchanges[i].Partition, exchanges[i].Offset = 1, int64(i)
	}
	if err := down.ProcessBatch(ctx, exchanges); err != nil {
		t.Fatal(err)
	}
	out := backend.Outputs(downID)
	if len(out) != len(rows) {
		t.Fatal("exchange dropped null or empty data", out)
	}
	for i, r := range out {
		if !reflect.DeepEqual(r.Key, rows[i].Key) || !reflect.DeepEqual(r.Value, rows[i].Value) || r.Timestamp != rows[i].Timestamp {
			t.Fatal("exchange changed null/empty/timestamp identity", i, r)
		}
	}
}

type exchangeGate struct {
	*memory.Backend
	err   error
	seen  []string
	calls int
}

type transportHiddenBackend struct{ execution.Backend }

func TestRepartitionRejectsBackendWrappersWithoutTransportCapability(t *testing.T) {
	b := stream.NewStreamsBuilder()
	stream.Stream(b, "in", serde.String, serde.String).Repartition("exchange").To("out")
	plan := build(t, b)
	backend := memory.New()
	if _, err := plan.NewTask(ctx, spec(), transportHiddenBackend{Backend: backend}); err == nil {
		t.Fatal("wrapper silently bypassed internal routing validation")
	}
	grouped, err := execution.NewGroup(&atomicTransportHiddenBackend{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.NewTask(ctx, spec(), grouped); err == nil {
		t.Fatal("transaction group silently bypassed missing transport capability")
	}
}

type atomicTransportHiddenBackend struct{ execution.Backend }

func (b *atomicTransportHiddenBackend) CommitTasks(ctx context.Context, rows []execution.TaskTransaction) error {
	return b.Backend.(execution.AtomicBackend).CommitTasks(ctx, rows)
}

func (g *exchangeGate) ValidateRepartitionTopics(topics []string) error {
	g.calls++
	g.seen = append([]string(nil), topics...)
	return g.err
}

func TestExchangeConfigurationFailureCannotFenceHealthyTask(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		backend := &exchangeGate{Backend: memory.New(), err: errors.New("missing exchange routing")}
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
		plain := stream.NewStreamsBuilder()
		stream.Stream(plain, "in", serde.String, serde.String).To("out")
		healthy := task(t, build(t, plain), spec(), storage)
		defer healthy.Close()
		b := stream.NewStreamsBuilder()
		stream.Stream(b, "in", serde.String, serde.String).Repartition("exchange").To("out")
		if _, err := build(t, b).NewTask(ctx, spec(), storage); !errors.Is(err, backend.err) {
			t.Fatal("exchange routing validation was skipped", err)
		}
		if backend.calls != 1 || !reflect.DeepEqual(backend.seen, []string{"test-exchange-repartition"}) {
			t.Fatal("group failed to forward exchange capability", backend.calls, backend.seen)
		}
		var err error
		if group == nil {
			err = healthy.Process(ctx, record.Bytes{Topic: "in"})
		} else {
			err = group.Process(ctx, []execution.TaskBatch{{Task: healthy, Records: []record.Bytes{{Topic: "in"}}}})
		}
		if err != nil {
			t.Fatal("invalid exchange setup fenced healthy owner", err)
		}
	}
}

func TestResolvedExchangeTopicsAreOwnedAndRejectCollisions(t *testing.T) {
	for _, conflict := range []string{"none", "source", "sink", "invalid"} {
		b := stream.NewStreamsBuilder()
		source := stream.Stream(b, "in", serde.String, serde.String)
		name := "exchange"
		if conflict == "invalid" {
			name = "bad space"
		}
		out := source.Repartition(name)
		if conflict == "source" {
			stream.Stream(b, "test-exchange-repartition", serde.String, serde.String)
		}
		if conflict == "sink" {
			out.To("test-exchange-repartition")
		} else {
			out.To("out")
		}
		plan := build(t, b)
		topics, err := plan.InputTopics("test")
		if conflict != "none" {
			if err == nil {
				t.Fatal("invalid exchange topics accepted", conflict)
			}
			continue
		}
		if err != nil || !reflect.DeepEqual(topics, []string{"in", "test-exchange-repartition"}) {
			t.Fatal(topics, err)
		}
		topics[0] = "mutated"
		again, _ := plan.InputTopics("test")
		exchanges, _ := plan.RepartitionTopics("test")
		if again[0] != "in" || !reflect.DeepEqual(exchanges, []string{"test-exchange-repartition"}) {
			t.Fatal("topic metadata shared mutable storage", again, exchanges)
		}
	}
}
