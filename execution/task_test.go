package execution_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

var ctx = context.Background()
var injected = errors.New("injected commit failure")

type failureBackend struct {
	execution.Backend
	after bool
	fail  bool
}

func (b *failureBackend) Commit(c context.Context, s execution.Spec, g uint64, tx execution.Transaction) error {
	if !b.fail {
		return b.Backend.Commit(c, s, g, tx)
	}
	b.fail = false
	if b.after {
		if e := b.Backend.Commit(c, s, g, tx); e != nil {
			return e
		}
	}
	return injected
}
func TestAbortAndAmbiguousCommitRecovery(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			b := stream.NewStreamsBuilder()
			stream.Count(stream.Stream(b, "in", serde.String, serde.String), "count").ToStream().To("out")
			p, e := b.Build()
			if e != nil {
				t.Fatal(e)
			}
			memoryBackend := memory.New()
			backend := &failureBackend{memoryBackend, after, true}
			id := execution.Spec{ApplicationID: "test", ID: "0"}
			a, e := p.NewTask(ctx, id, backend)
			if e != nil {
				t.Fatal(e)
			}
			r := record.Bytes{Topic: "in", Timestamp: 0, Offset: 0, Key: []byte("a"), Value: []byte("x")}
			if e = a.Process(ctx, r); !errors.Is(e, injected) {
				t.Fatal(e)
			}
			if _, ok, _ := a.StoreGet("count", "a"); ok {
				t.Fatal("unacknowledged local state published")
			}
			if e = a.Process(ctx, r); !errors.Is(e, execution.ErrPoisoned) {
				t.Fatalf("failed task reused: %v", e)
			}
			a, e = p.NewTask(ctx, id, backend)
			if e != nil {
				t.Fatal(e)
			}
			if e = a.Process(ctx, r); e != nil {
				t.Fatal(e)
			} // retry or skip according to committed offsets
			r.Offset = 1
			if e = a.Process(ctx, r); e != nil {
				t.Fatal(e)
			}
			out := memoryBackend.Outputs(id)
			if len(out) != 2 {
				t.Fatalf("outputs=%d", len(out))
			}
			v, e := serde.Int64.Decode(out[1].Value)
			if e != nil || v != 2 {
				t.Fatalf("count=%d %v", v, e)
			}
		})
	}
}

func TestConcurrentEntryRejected(t *testing.T) {
	a, e := execution.New(ctx, execution.Spec{ApplicationID: "a", ID: "0"}, memory.New())
	if e != nil {
		t.Fatal(e)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	if e = a.Register("in", func(record.Bytes) error { close(entered); <-release; return nil }); e != nil {
		t.Fatal(e)
	}
	r := record.Bytes{Topic: "in", Offset: 0}
	go func() { done <- a.Process(ctx, r) }()
	<-entered
	if e = a.Process(ctx, r); !errors.Is(e, execution.ErrConcurrent) {
		t.Errorf("concurrent entry: %v", e)
	}
	close(release)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}

func TestInputValidationAndCancellation(t *testing.T) {
	a, e := execution.New(ctx, execution.Spec{ApplicationID: "a", ID: "0", Partition: 1}, memory.New())
	if e != nil {
		t.Fatal(e)
	}
	if e = a.Register("in", func(record.Bytes) error { return nil }); e != nil {
		t.Fatal(e)
	}
	for _, r := range []record.Bytes{{Topic: "unknown", Partition: 1}, {Topic: "in", Partition: 0}, {Topic: "in", Partition: 1, Timestamp: -1}, {Topic: "in", Partition: 1, Offset: -1}} {
		if e = a.Process(ctx, r); e == nil {
			t.Fatalf("invalid input accepted: %+v", r)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if e = a.Process(cancelled, record.Bytes{Topic: "in", Partition: 1}); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e = a.Process(ctx, record.Bytes{Topic: "in", Partition: 1}); e != nil {
		t.Fatal("validation/cancellation poisoned task", e)
	}
}

func TestSchedulerOrderingIsolationAndBounds(t *testing.T) {
	for _, strategy := range []execution.Strategy{execution.BoundedWorkers, execution.GoroutinePerTask} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			var active, peak atomic.Int64
			var lanes []execution.Lane
			for i := 0; i < 16; i++ {
				a, e := execution.New(ctx, execution.Spec{ApplicationID: "schedule", ID: fmt.Sprint(i)}, memory.New())
				if e != nil {
					t.Fatal(e)
				}
				expected := int64(0)
				if e = a.Register("in", func(r record.Bytes) error {
					n := active.Add(1)
					defer active.Add(-1)
					for {
						old := peak.Load()
						if n <= old || peak.CompareAndSwap(old, n) {
							break
						}
					}
					if r.Offset != expected {
						return fmt.Errorf("out of order: got %d want %d", r.Offset, expected)
					}
					expected++
					return nil
				}); e != nil {
					t.Fatal(e)
				}
				var records []record.Bytes
				for j := 0; j < 100; j++ {
					records = append(records, record.Bytes{Topic: "in", Offset: int64(j)})
				}
				lanes = append(lanes, execution.Lane{Task: a, Records: records})
			}
			if e := execution.Run(ctx, lanes, 3, strategy); e != nil {
				t.Fatal(e)
			}
			if strategy == execution.BoundedWorkers && peak.Load() > 3 {
				t.Fatalf("worker limit exceeded: %d", peak.Load())
			}
			if e := execution.Run(ctx, []execution.Lane{lanes[0], lanes[0]}, 3, strategy); e == nil {
				t.Fatal("duplicate lane accepted")
			}
		})
	}
}
func TestSchedulerErrorAndCancellation(t *testing.T) {
	a, e := execution.New(ctx, execution.Spec{ApplicationID: "a", ID: "0"}, memory.New())
	if e != nil {
		t.Fatal(e)
	}
	if e = a.Register("in", func(record.Bytes) error { return injected }); e != nil {
		t.Fatal(e)
	}
	lanes := []execution.Lane{{Task: a, Records: []record.Bytes{{Topic: "in"}}}}
	if e := execution.Run(ctx, lanes, 1, execution.BoundedWorkers); !errors.Is(e, injected) {
		t.Fatal(e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if e := execution.Run(cancelled, lanes, 1, execution.BoundedWorkers); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e := execution.Run(ctx, lanes, 0, execution.BoundedWorkers); e == nil {
		t.Fatal("zero workers accepted")
	}
}

func TestPanicPoisonsTaskAndDiscardsStaging(t *testing.T) {
	backend := memory.New()
	a, e := execution.New(ctx, execution.Spec{ApplicationID: "panic", ID: "0"}, backend)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.Register("in", func(record.Bytes) error { a.StorePut("state", "a", []byte("staged")); panic("boom") }); e != nil {
		t.Fatal(e)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic disappeared")
			}
		}()
		_ = a.Process(ctx, record.Bytes{Topic: "in"})
	}()
	if _, ok, _ := a.StoreGet("state", "a"); ok {
		t.Fatal("panic published state")
	}
	if e = a.Process(ctx, record.Bytes{Topic: "in"}); !errors.Is(e, execution.ErrPoisoned) {
		t.Fatal(e)
	}
}
