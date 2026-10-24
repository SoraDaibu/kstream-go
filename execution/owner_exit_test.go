package execution_test

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/SoraDaibu/kstream-go/adapters/memory"
	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
)

func TestSingleTaskOwnerExitPoisonsUncommittedIncarnation(t *testing.T) {
	backend := memory.New()
	spec := execution.Spec{ApplicationID: "owner-exit", ID: "0"}
	task, err := execution.New(ctx, spec, backend, execution.WithRecordMetadata())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Register("in", func(record.Bytes) error {
		task.StorePut("s", "key", []byte("uncommitted"))
		runtime.Goexit()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = task.Process(ctx, record.Bytes{Topic: "in"}) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("owner did not exit")
	}
	if _, _, err := task.StoreGet("s", "key"); !errors.Is(err, execution.ErrPoisoned) {
		t.Fatal("terminated owner left a reusable task", err)
	}
	if meta, present := task.RecordMetadata(); present || meta != (record.Metadata{}) {
		t.Fatal("terminated owner retained record metadata", meta)
	}
	if err := task.Close(); err != nil {
		t.Fatal("terminated owner retained guard", err)
	}
	_, snapshot, err := backend.Open(ctx, spec)
	if err != nil || len(snapshot.State) != 0 || len(backend.Outputs(spec)) != 0 {
		t.Fatal("owner exit published uncommitted state", err)
	}
}

func TestGroupOwnerExitPoisonsEveryParticipant(t *testing.T) {
	backend := &atomicProbe{Backend: memory.New()}
	group := newGroup(t, backend)
	tasks := groupTasks(t, group, func(part int, _ record.Bytes) error {
		if part == 1 {
			runtime.Goexit()
		}
		return nil
	}, func(int) []execution.Option { return []execution.Option{execution.WithRecordMetadata()} })
	done := make(chan struct{})
	go func() { defer close(done); _ = group.Process(ctx, groupRows(tasks, 0)) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("group owner did not exit")
	}
	if backend.calls != 0 {
		t.Fatal("terminated owner reached durable commit")
	}
	for i, task := range tasks {
		if meta, present := task.RecordMetadata(); present || meta != (record.Metadata{}) {
			t.Fatal("terminated group participant retained record metadata", meta)
		}
		if _, _, err := task.StoreGet("counts", "key"); !errors.Is(err, execution.ErrPoisoned) {
			t.Fatal("group participant remained reusable", err)
		}
		if len(backend.Outputs(groupSpec(i))) != 0 {
			t.Fatal("terminated group published output")
		}
		if err := task.Close(); err != nil {
			t.Fatal("terminated group retained guard", err)
		}
	}
}
