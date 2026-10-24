package stream_test

import (
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/processor"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/state"
	"github.com/SoraDaibu/kstream-go/stream"
)

type setupStore struct {
	*state.Memory
	closes int
}

func (s *setupStore) Close() error { s.closes++; return s.Memory.Close() }

func TestProcessorFactoryPanicReleasesUnpublishedStore(t *testing.T) {
	b := stream.NewStreamsBuilder()
	failure := &struct{ cause string }{cause: "factory failed"}
	stream.Stream(b, "in", serde.String, serde.String).Process("bad", false,
		func(processor.Context, processor.Forward[string, string]) processor.Handler[string, string] {
			panic(failure)
		})
	plan := build(t, b)
	storage := &setupStore{Memory: state.NewMemory()}
	var caught any
	func() {
		defer func() { caught = recover() }()
		_, _ = plan.NewTask(ctx, spec(), memory.New(), execution.WithStoreFactory(func(execution.Spec) (state.Store, error) { return storage, nil }))
	}()
	if caught != failure || storage.closes != 1 {
		t.Fatal("factory panic was changed or store ownership leaked", caught, storage.closes)
	}
}

func TestZeroTopologyCannotFenceHealthyTask(t *testing.T) {
	b := stream.NewStreamsBuilder()
	stream.Stream(b, "in", serde.String, serde.String).To("out")
	backend := memory.New()
	a := task(t, build(t, b), spec(), backend)
	defer a.Close()
	if _, err := (stream.Topology{}).NewTask(ctx, spec(), backend); err == nil {
		t.Fatal("zero topology admitted task")
	}
	if err := a.Process(ctx, record.Bytes{Topic: "in", Key: []byte("a"), Value: []byte("x")}); err != nil {
		t.Fatal("invalid topology revoked existing task", err)
	}
}
