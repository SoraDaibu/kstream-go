package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type produceCall struct {
	record *kgo.Record
	done   func(*kgo.Record, error)
}

type controlledProducer struct {
	queued chan produceCall
	seen   context.Context
}

func (*controlledProducer) ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults {
	panic("async producer used sync fallback")
}
func (p *controlledProducer) Produce(ctx context.Context, r *kgo.Record, done func(*kgo.Record, error)) {
	p.seen = ctx
	p.queued <- produceCall{r, done}
}

func TestProduceAllWaitsForConcurrentOutOfOrderErrorsAndCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		ctx, stop := context.WithCancel(context.Background())
		defer stop()
		const count = 16
		p := &controlledProducer{queued: make(chan produceCall, count)}
		rows := make([]*kgo.Record, count)
		for i := range rows {
			rows[i] = &kgo.Record{Topic: "out", Offset: int64(i)}
		}
		result := make(chan error, 1)
		go func() { result <- produceAll(ctx, p, rows) }()
		calls := make([]produceCall, count)
		for i := range calls {
			calls[i] = <-p.queued
			if calls[i].record != rows[i] {
				t.Fatal("producer replaced or reordered submitted record pointers")
			}
		}
		boom := errors.New("first completion failed")
		if cancel {
			stop()
			boom = context.Canceled
		}
		calls[7].done(calls[7].record, boom)
		var wg sync.WaitGroup
		// A bounded set of workers completes records in reverse order. Record 0
		// remains pending after errors/cancellation to detect premature return.
		for worker := 0; worker < 3; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := count - 1 - worker; i > 0; i -= 3 {
					if i == 7 {
						continue
					}
					calls[i].done(calls[i].record, errors.New("later error"))
				}
			}()
		}
		wg.Wait()
		select {
		case err := <-result:
			t.Fatal("returned before every callback completed", err)
		default:
		}
		calls[0].done(calls[0].record, nil)
		if err := <-result; !errors.Is(err, boom) || p.seen != ctx {
			t.Fatal("first error or original context lost", err)
		}
	}
}

type immediateProducer struct{ seen int }

func (p *immediateProducer) Produce(_ context.Context, r *kgo.Record, done func(*kgo.Record, error)) {
	p.seen++
	done(r, nil)
}
func (*immediateProducer) ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults {
	panic("immediate producer used sync fallback")
}

func TestProduceAllImmediateAndZeroCallbacks(t *testing.T) {
	p := &immediateProducer{}
	for _, rows := range [][]*kgo.Record{nil, {{Topic: "out"}}, {{Topic: "out"}, {Topic: "out"}}} {
		if err := produceAll(context.Background(), p, rows); err != nil {
			t.Fatal(err)
		}
	}
	if p.seen != 3 {
		t.Fatal("empty produce or immediate completion count mismatch", p.seen)
	}
	session := &fakeGroupSession{produceErr: errors.New("fallback")}
	if err := produceAll(context.Background(), session, []*kgo.Record{{Topic: "out"}}); err != session.produceErr || len(session.rows) != 1 {
		t.Fatal("sync-only compatibility path changed", err)
	}
}

type asyncGroupSession struct {
	*fakeGroupSession
	calls     chan produceCall
	completed atomic.Int32
}

func (s *asyncGroupSession) Produce(_ context.Context, r *kgo.Record, done func(*kgo.Record, error)) {
	s.calls <- produceCall{r, func(r *kgo.Record, err error) { s.completed.Add(1); done(r, err) }}
}
func (s *asyncGroupSession) End(ctx context.Context, action kgo.TransactionEndTry) (bool, error) {
	if s.completed.Load() != 8 {
		return false, errors.New("transaction finalized with pending records")
	}
	return s.fakeGroupSession.End(ctx, action)
}

func TestGroupTransactionWaitsForAllProduceCompletionsBeforeDecision(t *testing.T) {
	for _, fail := range []bool{false, true} {
		backend, fallback, txs := groupBackendFixture()
		session := &asyncGroupSession{fakeGroupSession: fallback, calls: make(chan produceCall, 8)}
		backend.session = session
		result := make(chan error, 1)
		go func() { result <- backend.CommitTasks(context.Background(), txs) }()
		calls := make([]produceCall, 8)
		for i := range calls {
			calls[i] = <-session.calls
		}
		var cause error
		if fail {
			cause = errors.New("produce failed")
		}
		calls[7].done(calls[7].record, cause)
		for i := 6; i >= 0; i-- {
			calls[i].done(calls[i].record, nil)
		}
		if err := <-result; !errors.Is(err, cause) {
			t.Fatal("transaction completion decision lost error", err, cause)
		}
		want := kgo.TryCommit
		if fail {
			want = kgo.TryAbort
		}
		if len(fallback.rows) != 0 || len(fallback.ends) != 1 || fallback.ends[0] != want {
			t.Fatal("wrong production path or transaction decision", fallback.ends)
		}
	}
}
