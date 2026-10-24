package stream_test

import (
	"errors"
	"fmt"
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

func TestHoppingClosureRecoveryAndAtomicFanout(t *testing.T) {
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
			stream.HoppingWindowedCount(stream.Stream(b, "in", serde.String, serde.String), "hops", window.Hopping{Size: 10, Advance: 5, Grace: 5}).To("out")
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
			feed(t, a, "in", 0, 9, []byte("a"), nil)    // Null values count in both windows.
			feed(t, a, "in", 1, 1000, nil, []byte("x")) // Null key cannot advance operator time.
			feed(t, a, "in", 2, 15, []byte("b"), []byte("x"))
			rows := []record.Bytes{{Topic: "in", Offset: 3, Timestamp: 9, Key: []byte("a"), Value: []byte("x")}, {Topic: "in", Offset: 4, Timestamp: 21, Key: []byte("c"), Value: []byte("x")}}
			backend.fail = true
			if err := a.ProcessBatch(ctx, rows); !errors.Is(err, windowFailure) {
				t.Fatal(err)
			}
			if len(backend.Outputs(spec())) != 4 {
				t.Fatal("aborted overlapping windows became visible")
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
			feed(t, a, "in", 5, 14, []byte("b"), []byte("late"))
			out := backend.Outputs(spec())
			want := []struct {
				key                     string
				start, value, timestamp int64
			}{
				{"a", 0, 1, 9}, {"a", 5, 1, 9}, {"b", 10, 1, 15}, {"b", 15, 1, 15},
				{"a", 5, 2, 9}, {"c", 15, 1, 21}, {"c", 20, 1, 21}, {"b", 10, 2, 15},
			}
			if len(out) != len(want) {
				t.Fatal("lost/duplicated hopping output", len(out))
			}
			for i, r := range out {
				key, err := serde.JSON[window.Key[string]]().Decode(r.Key)
				v := want[i]
				if err != nil || key.Key != v.key || key.Start != v.start || key.End != v.start+10 || long(t, r.Value) != v.value || r.Timestamp != v.timestamp {
					t.Fatal("wrong hopping update", i, key, r)
				}
			}
		})
	}
}
