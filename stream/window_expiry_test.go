package stream_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/adapters/pebble"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
	"github.com/SoraDaibu/kstream-go/window"
)

type windowFailureBackend struct {
	*memory.Backend
	fail bool
}

func TestWindowExpiryRestoresUnorderedWindowsAndRemovesEarliest(t *testing.T) {
	b := stream.NewStreamsBuilder()
	stream.WindowedCount(stream.Stream(b, "in", serde.String, serde.String), "windows", window.Tumbling{Size: 10, Grace: 1000}).To("out")
	plan := build(t, b)
	backend := memory.New()
	a := task(t, plan, spec(), backend)
	for offset, start := range rand.New(rand.NewSource(73019)).Perm(100) {
		feed(t, a, "in", int64(offset), int64(start*10), []byte("key"), []byte("x"))
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a = task(t, plan, spec(), backend)
	defer a.Close()
	feed(t, a, "in", 100, 1010, []byte("key"), []byte("x"))
	starts := func() map[int64]bool {
		retained := make(map[int64]bool)
		if err := a.StoreScan("windows", func(key string, _ []byte) {
			if len(key) >= 8 {
				retained[int64(binary.BigEndian.Uint64([]byte(key[:8])))] = true
			}
		}); err != nil {
			t.Fatal(err)
		}
		return retained
	}
	retained := starts()
	if len(retained) != 100 || retained[0] || !retained[10] || !retained[1010] {
		t.Fatal("restored expiry order lost windows", retained)
	}
	feed(t, a, "in", 101, 2000, []byte("key"), []byte("x"))
	retained = starts()
	if len(retained) != 2 || !retained[1010] || !retained[2000] {
		t.Fatal("bulk expiry retained closed windows", retained)
	}
}

var windowFailure = errors.New("window commit rejected")

func (b *windowFailureBackend) Commit(ctx context.Context, spec execution.Spec, generation uint64, tx execution.Transaction) error {
	if b.fail {
		return windowFailure
	}
	return b.Backend.Commit(ctx, spec, generation, tx)
}

func TestWindowExpiryRebuildAfterAbortedEvictionBatch(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprintf("persistent=%t", persistent), func(t *testing.T) {
			var options []execution.Option
			if persistent {
				engine, err := pebble.Open(filepath.Join(t.TempDir(), "state"), pebble.Config{SyncWrites: true})
				if err != nil {
					t.Fatal(err)
				}
				defer engine.Close()
				options = append(options, execution.WithStoreFactory(engine.Task))
			}
			b := stream.NewStreamsBuilder()
			stream.WindowedCount(stream.Stream(b, "in", serde.String, serde.String), "windows", window.Tumbling{Size: 10, Grace: 5}).To("out")
			plan := build(t, b)
			backend := &windowFailureBackend{Backend: memory.New()}
			open := func() *execution.Task {
				a, err := plan.NewTask(ctx, spec(), backend, options...)
				if err != nil {
					t.Fatal(err)
				}
				return a
			}
			a := open()
			for i := 0; i < 100; i++ {
				feed(t, a, "in", int64(i), 1, []byte(fmt.Sprintf("key-%d", i)), []byte("x"))
			}
			rows := []record.Bytes{
				{Topic: "in", Offset: 100, Timestamp: 9, Key: []byte("key-0"), Value: nil},
				{Topic: "in", Offset: 101, Timestamp: 15, Key: []byte{}, Value: []byte("x")},
			}
			backend.fail = true
			if err := a.ProcessBatch(ctx, rows); !errors.Is(err, windowFailure) {
				t.Fatal(err)
			}
			if err := a.ProcessBatch(ctx, rows); !errors.Is(err, execution.ErrPoisoned) {
				t.Fatal(err)
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			backend.fail = false
			a = open()
			defer a.Close()
			if err := a.ProcessBatch(ctx, rows); err != nil {
				t.Fatal(err)
			}
			if err := a.ProcessBatch(ctx, rows); err != nil {
				t.Fatal(err)
			}
			out := backend.Outputs(spec())
			if len(out) != 102 || long(t, out[100].Value) != 2 || out[100].Timestamp != 9 || out[101].Key == nil {
				t.Fatal("recovery lost pre-abort window state or duplicated output")
			}
			assertOnlyStart := func(want int64) {
				entries := 0
				if err := a.StoreScan("windows", func(key string, _ []byte) {
					if key == "" {
						return
					}
					entries++
					if len(key) < 8 || int64(binary.BigEndian.Uint64([]byte(key[:8]))) != want {
						t.Errorf("expired window survived: %x", key)
					}
				}); err != nil {
					t.Fatal(err)
				}
				if entries != 1 {
					t.Fatal("unexpected retained windows", entries)
				}
			}
			assertOnlyStart(10)
			feed(t, a, "in", 102, 25, []byte("next"), []byte("x"))
			assertOnlyStart(20)
		})
	}
}
