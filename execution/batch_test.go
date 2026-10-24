package execution_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

type recordingBackend struct {
	execution.Backend
	commits int
	last    execution.Transaction
}

func (b *recordingBackend) Commit(c context.Context, s execution.Spec, g uint64, tx execution.Transaction) error {
	b.commits++
	b.last = tx
	return b.Backend.Commit(c, s, g, tx)
}
func TestBatchStateOffsetsReplayAndSingleCommit(t *testing.T) {
	b := stream.NewStreamsBuilder()
	stream.Count(stream.Stream(b, "in", serde.String, serde.String), "counts").ToStream().To("out")
	plan, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	log := memory.New()
	backend := &recordingBackend{Backend: log}
	spec := execution.Spec{ApplicationID: "batch", ID: "0"}
	task, err := plan.NewTask(ctx, spec, backend)
	if err != nil {
		t.Fatal(err)
	}
	records := []record.Bytes{
		{Topic: "in", Offset: 0, Timestamp: 1, Key: []byte("a"), Value: []byte("x")},
		{Topic: "in", Offset: 0, Timestamp: 2, Key: []byte("a"), Value: []byte("duplicate")},
		{Topic: "in", Offset: 1, Timestamp: 3, Key: []byte("a"), Value: []byte("x")},
		{Topic: "in", Offset: 2, Timestamp: 4, Key: []byte("b"), Value: []byte("x")},
	}
	if err = task.ProcessBatch(ctx, records); err != nil {
		t.Fatal(err)
	}
	if backend.commits != 1 || len(backend.last.Mutations) != 2 || backend.last.NextOffsets["in"] != 3 {
		t.Fatalf("unexpected transaction: %+v commits=%d", backend.last, backend.commits)
	}
	var counts []int64
	for _, r := range log.Outputs(spec) {
		n, e := serde.Int64.Decode(r.Value)
		if e != nil {
			t.Fatal(e)
		}
		counts = append(counts, n)
	}
	if !reflect.DeepEqual(counts, []int64{1, 2, 1}) {
		t.Fatal(counts)
	}
	task, err = plan.NewTask(ctx, spec, backend)
	if err != nil {
		t.Fatal(err)
	}
	if err = task.ProcessBatch(ctx, records); err != nil {
		t.Fatal(err)
	}
	if backend.commits != 1 {
		t.Fatal("replay-only batch committed")
	}
	next := records[0]
	next.Offset = 3
	if err = task.Process(ctx, next); err != nil {
		t.Fatal(err)
	}
	out := log.Outputs(spec)
	n, e := serde.Int64.Decode(out[len(out)-1].Value)
	if e != nil || n != 3 {
		t.Fatalf("restored count %d: %v", n, e)
	}
}

func TestBatchPreservesClockAndCrossTopicStateOrder(t *testing.T) {
	backend := &recordingBackend{Backend: memory.New()}
	spec := execution.Spec{ApplicationID: "ordered", ID: "0"}
	task, err := execution.New(ctx, spec, backend)
	if err != nil {
		t.Fatal(err)
	}
	var clocks []int64
	if err = task.Register("table", func(r record.Bytes) error { task.StorePut("lookup", string(r.Key), r.Value); return nil }); err != nil {
		t.Fatal(err)
	}
	if err = task.Register("events", func(r record.Bytes) error {
		clocks = append(clocks, task.StreamTime())
		v, ok, err := task.StoreGet("lookup", string(r.Key))
		if err != nil {
			return err
		}
		if ok {
			task.Emit(record.Bytes{Topic: "out", Key: r.Key, Value: v})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rows := []record.Bytes{
		{Topic: "table", Offset: 0, Timestamp: 1, Key: []byte("a"), Value: []byte("A")},
		{Topic: "events", Offset: 0, Timestamp: 2, Key: []byte("a")},
		{Topic: "table", Offset: 1, Timestamp: 100, Key: []byte("a"), Value: nil},
		{Topic: "events", Offset: 1, Timestamp: 3, Key: []byte("a")},
	}
	if err = task.ProcessBatch(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(clocks, []int64{2, 100}) {
		t.Fatalf("future clock leaked into earlier record: %v", clocks)
	}
	if len(backend.last.Outputs) != 1 || string(backend.last.Outputs[0].Value) != "A" {
		t.Fatalf("staged lookup: %+v", backend.last.Outputs)
	}
	if len(backend.last.Mutations) != 1 || backend.last.Mutations[0].Value != nil {
		t.Fatal("final tombstone was lost")
	}
}

func TestBatchFailureAndCancellationPublishNothing(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "callback", true: "cancellation"}[cancelled], func(t *testing.T) {
			log := memory.New()
			backend := &recordingBackend{Backend: log}
			spec := execution.Spec{ApplicationID: "abort", ID: "0"}
			task, err := execution.New(ctx, spec, backend)
			if err != nil {
				t.Fatal(err)
			}
			batchCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if err = task.Register("in", func(r record.Bytes) error {
				task.StorePut("state", "a", r.Value)
				task.Emit(record.Bytes{Topic: "out", Value: r.Value})
				if cancelled {
					cancel()
				} else if r.Offset == 1 {
					return injected
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			err = task.ProcessBatch(batchCtx, []record.Bytes{{Topic: "in", Offset: 0, Value: []byte("one")}, {Topic: "in", Offset: 1, Value: []byte("two")}})
			want := injected
			if cancelled {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("error %v want %v", err, want)
			}
			if backend.commits != 0 || len(log.Outputs(spec)) != 0 {
				t.Fatal("aborted batch escaped")
			}
			if _, ok, _ := task.StoreGet("state", "a"); ok {
				t.Fatal("aborted local state escaped")
			}
			if err = task.Process(ctx, record.Bytes{Topic: "in"}); !errors.Is(err, execution.ErrPoisoned) {
				t.Fatal(err)
			}
		})
	}
}

func TestBatchValidationRunsBeforeCallbacks(t *testing.T) {
	task, err := execution.New(ctx, execution.Spec{ApplicationID: "validation", ID: "0"}, memory.New())
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	if err = task.Register("in", func(record.Bytes) error { called++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err = task.ProcessBatch(ctx, []record.Bytes{{Topic: "in"}, {Topic: "missing", Offset: 1}}); err == nil {
		t.Fatal("invalid batch accepted")
	}
	if called != 0 {
		t.Fatal("validation invoked callbacks")
	}
	if err = task.Process(ctx, record.Bytes{Topic: "in"}); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatal("validation poisoned task")
	}
}

func TestBatchAmbiguousCommitRecovery(t *testing.T) {
	b := stream.NewStreamsBuilder()
	stream.Count(stream.Stream(b, "in", serde.String, serde.String), "counts").ToStream().To("out")
	plan, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	log := memory.New()
	backend := &failureBackend{Backend: log, after: true, fail: true}
	spec := execution.Spec{ApplicationID: "ambiguous-batch", ID: "0"}
	task, err := plan.NewTask(ctx, spec, backend)
	if err != nil {
		t.Fatal(err)
	}
	records := []record.Bytes{{Topic: "in", Offset: 0, Key: []byte("a"), Value: []byte("x")}, {Topic: "in", Offset: 1, Key: []byte("a"), Value: []byte("x")}}
	if err = task.ProcessBatch(ctx, records); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	task, err = plan.NewTask(ctx, spec, backend)
	if err != nil {
		t.Fatal(err)
	}
	if err = task.ProcessBatch(ctx, records); err != nil {
		t.Fatal(err)
	}
	if len(log.Outputs(spec)) != 2 {
		t.Fatal("ambiguous replay duplicated output")
	}
}

func TestBatchOverlayMatchesIndependentMapAcrossWritesDeletesAndScans(t *testing.T) {
	backend := memory.New()
	task, err := execution.New(ctx, execution.Spec{ApplicationID: "overlay", ID: "0"}, backend)
	if err != nil {
		t.Fatal(err)
	}
	expected := make(map[string]string)
	steps := 0
	if err = task.Register("in", func(r record.Bytes) error {
		key := string(r.Key)
		got, exists, err := task.StoreGet("state", key)
		if err != nil {
			return err
		}
		want, wantExists := expected[key]
		if exists != wantExists || string(got) != want {
			return errors.New("read-your-writes diverged from reference map")
		}
		task.StorePut("state", key, r.Value)
		if r.Value == nil {
			delete(expected, key)
		} else {
			expected[key] = string(r.Value)
		}
		steps++
		if steps%17 == 0 {
			actual := make(map[string]string)
			task.StoreScan("state", func(k string, v []byte) { actual[k] = string(v) })
			if !reflect.DeepEqual(actual, expected) {
				return errors.New("scan diverged from reference map")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// 64 distinct keys force the indexed path; repeated updates and tombstones
	// cover both sides of its promotion threshold and later batch publication.
	for batch := 0; batch < 8; batch++ {
		rows := make([]record.Bytes, 256)
		for i := range rows {
			key := []byte{byte('a' + i%64)}
			value := []byte{byte(batch), byte(i)}
			if (i+batch)%7 == 0 {
				value = nil
			}
			rows[i] = record.Bytes{Topic: "in", Offset: int64(batch*256 + i), Key: key, Value: value}
		}
		if err = task.ProcessBatch(ctx, rows); err != nil {
			t.Fatal(err)
		}
	}
	actual := make(map[string]string)
	task.StoreScan("state", func(k string, v []byte) { actual[k] = string(v) })
	if !reflect.DeepEqual(actual, expected) {
		t.Fatal("published state diverged from reference map")
	}
}
