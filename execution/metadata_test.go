package execution_test

import (
	"context"
	"errors"
	"testing"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
)

type metadataCommitProbe struct {
	*memory.Backend
	task    *execution.Task
	commits int
}

func TestRuntimeMetadataRequiresExplicitOptIn(t *testing.T) {
	a, err := execution.New(ctx, execution.Spec{ApplicationID: "metadata-disabled", ID: "0"}, memory.New())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Register("in", func(record.Bytes) error {
		if meta, present := a.RecordMetadata(); present || meta != (record.Metadata{}) {
			t.Fatal("runtime metadata enabled without opt-in", meta)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.Process(ctx, record.Bytes{Topic: "in"}); err != nil {
		t.Fatal(err)
	}
}

func (b *metadataCommitProbe) Commit(ctx context.Context, spec execution.Spec, generation uint64, tx execution.Transaction) error {
	if meta, present := b.task.RecordMetadata(); present || meta != (record.Metadata{}) {
		return errors.New("metadata exposed during backend commit")
	}
	b.commits++
	return b.Backend.Commit(ctx, spec, generation, tx)
}

func TestMetadataEndsBeforeCommitAndAfterCallbackFailures(t *testing.T) {
	for _, mode := range []string{"success", "error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			backend := &metadataCommitProbe{Backend: memory.New()}
			a, err := execution.New(ctx, execution.Spec{ApplicationID: "metadata", ID: "0"}, backend, execution.WithRecordMetadata())
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			backend.task = a
			failure := errors.New("callback failed")
			if err := a.Register("in", func(record.Bytes) error {
				meta, present := a.RecordMetadata()
				if !present || meta != (record.Metadata{Topic: "in", Offset: 7}) {
					t.Fatal("wrong callback source", meta, present)
				}
				switch mode {
				case "error":
					return failure
				case "panic":
					panic(failure)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			var panicValue any
			func() {
				defer func() { panicValue = recover() }()
				err = a.Process(ctx, record.Bytes{Topic: "in", Offset: 7})
			}()
			if mode == "success" && (err != nil || backend.commits != 1) {
				t.Fatal(err, backend.commits)
			}
			if mode == "error" && !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if mode == "panic" && panicValue != failure {
				t.Fatal(panicValue)
			}
			if meta, present := a.RecordMetadata(); present || meta != (record.Metadata{}) {
				t.Fatal("callback retained source metadata", meta)
			}
			if mode != "success" && backend.commits != 0 {
				t.Fatal("failed callback committed")
			}
		})
	}
}
