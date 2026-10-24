package execution_test

import (
	"context"
	"errors"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
)

type clockFailureBackend struct {
	*memory.Backend
	fail bool
}

func (b *clockFailureBackend) Commit(ctx context.Context, spec execution.Spec, generation uint64, tx execution.Transaction) error {
	if b.fail {
		return errors.New("rejected before commit")
	}
	return b.Backend.Commit(ctx, spec, generation, tx)
}

func TestClockCommitFailureDoesNotPublishProgress(t *testing.T) {
	b := &clockFailureBackend{Backend: memory.New()}
	spec := execution.Spec{ApplicationID: "scoped", ID: "0"}
	open := func() (*execution.Task, *execution.ScopedContext) {
		a, err := execution.New(ctx, spec, b)
		if err != nil {
			t.Fatal(err)
		}
		c := a.ScopedContext("stage")
		if err := a.Register("in", func(r record.Bytes) error { c.Observe(r.Timestamp); return nil }); err != nil {
			t.Fatal(err)
		}
		return a, c
	}
	a, clock := open()
	if err := a.Process(ctx, record.Bytes{Topic: "in", Timestamp: 9}); err != nil {
		t.Fatal(err)
	}
	b.fail = true
	if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: 1, Timestamp: 100}); err == nil {
		t.Fatal("commit failure hidden")
	}
	if clock.StreamTime() != 9 {
		t.Fatal("failed decision published clock", clock.StreamTime())
	}
	_ = a.Close()
	b.fail = false
	a, clock = open()
	defer a.Close()
	if clock.StreamTime() != 9 {
		t.Fatal("failed decision persisted clock", clock.StreamTime())
	}
	if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: 1, Timestamp: 3}); err != nil {
		t.Fatal(err)
	}
	if clock.StreamTime() != 9 {
		t.Fatal("clock regressed", clock.StreamTime())
	}
}

func TestRestoredClockValidationAndOwnership(t *testing.T) {
	for _, clocks := range []map[string]int64{{"": 1}, {"stage": -1}, {"stage": 43}, {"stage": 9}} {
		b := &streamingBackend{restore: func(context.Context, execution.RestoreSink) (execution.Checkpoint, error) {
			return execution.Checkpoint{StreamTime: 42, ScopeTimes: clocks}, nil
		}}
		a, err := execution.New(ctx, execution.Spec{ApplicationID: "clocks", ID: "0"}, b)
		if clocks["stage"] != 9 {
			if err == nil {
				_ = a.Close()
				t.Fatal("invalid restored clock accepted", clocks)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		clocks["stage"] = 999
		if a.ScopedContext("stage").StreamTime() != 9 {
			t.Fatal("task retained backend clock map")
		}
		_ = a.Close()
	}
}
