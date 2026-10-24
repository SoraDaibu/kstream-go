package stream_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/stream"
)

func TestTypedProcessorOwnsStateAcrossMergeRecoveryAndTasks(t *testing.T) {
	b := stream.NewStreamsBuilder()
	merged := stream.Merge(stream.Stream(b, "left", serde.String, serde.String), stream.Stream(b, "right", serde.String, serde.String), "merged")
	factories := 0
	var seen []record.Metadata
	stream.Process(merged, "numeric", serde.Int64, serde.Int64,
		func(c processor.Context, next processor.Forward[int64, int64]) processor.Handler[string, string] {
			factories++
			if _, present := c.RecordMetadata(); present {
				t.Fatal("metadata available during supplier initialization")
			}
			return func(r record.Record[string, string]) error {
				meta, present := c.RecordMetadata()
				if !present {
					t.Fatal("missing processor metadata")
				}
				seen = append(seen, meta)
				data, exists, err := c.StoreGet("custom", "seen")
				if err != nil {
					return err
				}
				n := int64(1)
				if exists {
					n += long(t, data)
				}
				encoded, _ := serde.Int64.Encode(n)
				c.StorePut("custom", "seen", encoded)
				out := record.Record[int64, int64]{Key: int64(len(r.Key)), Value: n, NullKey: r.NullKey, Timestamp: r.Timestamp + 1}
				if err := next(out); err != nil {
					return err
				}
				out.NullValue = r.NullValue
				out.Timestamp++
				return next(out)
			}
		}).To("out")
	plan := build(t, b)
	backend := memory.New()
	first := task(t, plan, spec(), backend)
	defer first.Close()
	feed(t, first, "left", 0, 20, []byte("abc"), []byte{})
	feed(t, first, "right", 0, 5, nil, nil)
	if factories != 1 {
		t.Fatal("merged graph created duplicate processor owners", factories)
	}
	restored := task(t, plan, spec(), backend)
	defer restored.Close()
	feed(t, restored, "left", 0, 20, []byte("abc"), []byte{}) // replay
	feed(t, restored, "right", 1, 0, []byte{}, []byte("x"))
	otherID := spec()
	otherID.ID = "other"
	other := task(t, plan, otherID, backend)
	defer other.Close()
	feed(t, other, "left", 0, 1, []byte("z"), []byte("x"))
	out := backend.Outputs(spec())
	if factories != 3 || len(out) != 6 || long(t, backend.Outputs(otherID)[0].Value) != 1 {
		t.Fatal("supplier/state was shared across task incarnations", factories, len(out))
	}
	for i, want := range []int64{1, 1, 2, 0, 3, 3} {
		if i == 3 {
			if out[i].Value != nil {
				t.Fatal("null output became a numeric zero")
			}
		} else if long(t, out[i].Value) != want {
			t.Fatal("lost committed processor state", i, out[i])
		}
	}
	if long(t, out[0].Key) != 3 || out[2].Key != nil || out[3].Key != nil || long(t, out[4].Key) != 0 {
		t.Fatal("typed output null/empty key mismatch")
	}
	var timestamps []int64
	for _, r := range out {
		timestamps = append(timestamps, r.Timestamp)
	}
	if !reflect.DeepEqual(timestamps, []int64{21, 22, 6, 7, 1, 2}) {
		t.Fatal("processor timestamps were replaced", timestamps)
	}
	wantMetadata := []record.Metadata{{Topic: "left", Offset: 0}, {Topic: "right", Offset: 0}, {Topic: "right", Offset: 1}, {Topic: "left", Offset: 0}}
	if !reflect.DeepEqual(seen, wantMetadata) {
		t.Fatal("typed processor lost source identity or replayed input", seen)
	}
}

func TestTypedProcessorFailureRollsBackStateOutputsAndOffsets(t *testing.T) {
	for _, downstream := range []bool{false, true} {
		b := stream.NewStreamsBuilder()
		boom := errors.New("processor failure")
		fail := true
		out := stream.Process(stream.Stream(b, "in", serde.String, serde.String), "typed", serde.Int64, serde.Int64,
			func(c processor.Context, next processor.Forward[int64, int64]) processor.Handler[string, string] {
				return func(r record.Record[string, string]) error {
					c.StorePut("custom", "written", []byte("value"))
					if err := next(record.Record[int64, int64]{Value: 7, Timestamp: r.Timestamp}); err != nil {
						return err
					}
					if fail && !downstream {
						return boom
					}
					return nil
				}
			})
		out.To("out")
		out.Foreach("reject", func(record.Record[int64, int64]) error {
			if fail && downstream {
				return boom
			}
			return nil
		})
		plan := build(t, b)
		backend := memory.New()
		a := task(t, plan, spec(), backend)
		if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: 0, Timestamp: 3}); !errors.Is(err, boom) {
			t.Fatal("processor error lost", err)
		}
		if len(backend.Outputs(spec())) != 0 {
			t.Fatal("failed typed output escaped")
		}
		if err := a.Process(ctx, record.Bytes{Topic: "in", Offset: 1}); !errors.Is(err, execution.ErrPoisoned) {
			t.Fatal("failed processor owner remained usable", err)
		}
		_ = a.Close()
		fail = false
		restored := task(t, plan, spec(), backend)
		if _, exists, err := restored.StoreGet("custom", "written"); err != nil || exists {
			t.Fatal("failed processor state escaped", exists, err)
		}
		feed(t, restored, "in", 0, 3, nil, nil)
		if len(backend.Outputs(spec())) != 1 {
			t.Fatal("aborted input offset was incorrectly committed")
		}
		_ = restored.Close()
	}
}

func TestTypedProcessorValidatesCodecsSupplierAndPartitioning(t *testing.T) {
	supplier := func(_ processor.Context, next processor.Forward[int64, int64]) processor.Handler[string, string] {
		return func(record.Record[string, string]) error { return next(record.Record[int64, int64]{}) }
	}
	for _, kind := range []string{"supplier", "key", "value", "group"} {
		t.Run(kind, func(t *testing.T) {
			b := stream.NewStreamsBuilder()
			key, value := serde.Int64, serde.Int64
			var f processor.Supplier[string, string, int64, int64] = supplier
			switch kind {
			case "supplier":
				f = nil
			case "key":
				key = serde.Codec[int64]{}
			case "value":
				value = serde.Codec[int64]{}
			}
			out := stream.Process(stream.Stream(b, "in", serde.String, serde.String), "typed", key, value, f)
			if kind == "group" {
				stream.Count(out, "counts").ToStream().To("out")
			}
			if _, err := b.Build(); err == nil {
				t.Fatal("invalid typed processor topology accepted")
			}
		})
	}
}
