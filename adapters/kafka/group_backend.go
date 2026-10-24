package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/state"
	"github.com/twmb/franz-go/pkg/kgo"
)

var errGroupRebalanced = errors.New("Kafka group transaction aborted after ownership change")

type groupTransactor interface {
	Begin() error
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	End(context.Context, kgo.TransactionEndTry) (bool, error)
}
type groupTaskCheckpoint struct {
	generation uint64
	checkpoint execution.Checkpoint
	barrier    []byte
}
type groupBackend struct {
	config         Config
	application    string
	session        groupTransactor
	tracker        *assignmentTracker
	assignment     *groupAssignment
	entries        map[int32]groupTaskCheckpoint
	nextGeneration uint64
	pollNext       map[int32]map[string]int64
}

var _ execution.AtomicBackend = (*groupBackend)(nil)
var _ execution.StreamingRestorer = (*groupBackend)(nil)

func (b *groupBackend) reset(assignment *groupAssignment) {
	b.assignment = assignment
	b.entries = make(map[int32]groupTaskCheckpoint)
	b.pollNext = nil
}
func (b *groupBackend) owns(spec execution.Spec) bool {
	return spec.ApplicationID == b.application && spec.ID == "0" && b.assignment != nil && b.assignment.valid && b.assignment.partitions[spec.Partition]
}
func (b *groupBackend) Open(ctx context.Context, spec execution.Spec) (uint64, execution.Snapshot, error) {
	image := make(state.Image)
	generation, checkpoint, err := b.OpenInto(ctx, spec, func(rows []state.Mutation) error {
		for _, row := range rows {
			image.Apply(row)
		}
		return nil
	})
	if err != nil {
		return 0, execution.Snapshot{}, err
	}
	return generation, execution.Snapshot{State: image, NextOffsets: checkpoint.NextOffsets, StreamTime: checkpoint.StreamTime}, nil
}
func (b *groupBackend) OpenInto(ctx context.Context, spec execution.Spec, sink execution.RestoreSink) (uint64, execution.Checkpoint, error) {
	if !b.owns(spec) || b.tracker.current.Load() != b.assignment {
		return 0, execution.Checkpoint{}, errGroupRebalanced
	}
	restorer, err := New(b.config)
	if err != nil {
		return 0, execution.Checkpoint{}, err
	}
	defer restorer.Close()
	_, checkpoint, err := restorer.OpenInto(ctx, spec, sink)
	if err != nil {
		return 0, execution.Checkpoint{}, err
	}
	if b.tracker.current.Load() != b.assignment {
		return 0, execution.Checkpoint{}, errGroupRebalanced
	}
	b.nextGeneration++
	b.entries[spec.Partition] = groupTaskCheckpoint{generation: b.nextGeneration, checkpoint: checkpoint, barrier: record.CloneBytes(restorer.barrier)}
	return b.nextGeneration, checkpoint, nil
}
func (*groupBackend) Commit(context.Context, execution.Spec, uint64, execution.Transaction) error {
	return execution.ErrGroupRequired
}

func (b *groupBackend) abort(ctx context.Context, cause error) error {
	if _, err := b.session.End(ctx, kgo.TryAbort); err != nil {
		// Abort errors are fatal. Do not preserve a retryable rebalance sentinel
		// in the unwrap chain when the client reports an unsafe session error.
		return fmt.Errorf("abort after %v: %w", cause, err)
	}
	return cause
}
func (b *groupBackend) abortPoll(ctx context.Context) error {
	if err := b.session.Begin(); err != nil {
		return fenceError(err)
	}
	_, err := b.session.End(ctx, kgo.TryAbort)
	return fenceError(err)
}

func (b *groupBackend) CommitTasks(ctx context.Context, transactions []execution.TaskTransaction) error {
	if err := b.session.Begin(); err != nil {
		return fenceError(err)
	}
	if b.assignment == nil || !b.assignment.valid || b.tracker.current.Load() != b.assignment {
		return b.abort(ctx, errGroupRebalanced)
	}
	proposed := make(map[int32]execution.Checkpoint, len(transactions))
	for _, tx := range transactions {
		entry, ok := b.entries[tx.Spec.Partition]
		if !ok || !b.owns(tx.Spec) || tx.Generation != entry.generation {
			return b.abort(ctx, execution.ErrFenced)
		}
		if _, duplicate := proposed[tx.Spec.Partition]; duplicate {
			return b.abort(ctx, errors.New("duplicate group transaction participant"))
		}
		next := cloneOffsets(entry.checkpoint.NextOffsets)
		for topic, offset := range tx.Transaction.NextOffsets {
			next[topic] = offset
		}
		proposed[tx.Spec.Partition] = execution.Checkpoint{NextOffsets: next, StreamTime: tx.Transaction.StreamTime}
	}
	for partition, positions := range b.pollNext {
		entry, ok := b.entries[partition]
		if !ok {
			return b.abort(ctx, errors.New("polled task was not restored"))
		}
		checkpoint := entry.checkpoint
		if updated, ok := proposed[partition]; ok {
			checkpoint = updated
		}
		for topic, offset := range positions {
			if checkpoint.NextOffsets[topic] < offset {
				return b.abort(ctx, errors.New("transaction omitted polled input; refusing to advance group offsets"))
			}
		}
	}
	rowCount := len(transactions) // One complete checkpoint per changed task.
	for _, tx := range transactions {
		rowCount += len(tx.Transaction.Mutations) + len(tx.Transaction.Outputs)
	}
	for _, entry := range b.entries {
		if entry.barrier != nil {
			rowCount++
		}
	}
	// Fresh arrays own all record structs for this call. Do not reuse them after
	// produce completion: client hooks can retain individual record pointers.
	records := make([]kgo.Record, rowCount)
	rows := make([]*kgo.Record, 0, rowCount)
	appendRow := func(row kgo.Record) {
		i := len(rows)
		records[i] = row
		rows = append(rows, &records[i])
	}
	now := time.Now()
	for _, tx := range transactions {
		for _, mutation := range tx.Transaction.Mutations {
			appendRow(kgo.Record{Topic: b.config.ChangelogTopic, Partition: tx.Spec.Partition, Key: encodeKey(stateKind, tx.Spec, mutation.Store, mutation.Key), Value: mutation.Value, Timestamp: now})
		}
		for _, output := range tx.Transaction.Outputs {
			if output.Topic == b.config.ChangelogTopic {
				return b.abort(ctx, errors.New("user output cannot target the internal changelog"))
			}
			appendRow(kgo.Record{Topic: output.Topic, Partition: output.Partition, Key: output.Key, Value: output.Value, Timestamp: time.UnixMilli(output.Timestamp)})
		}
		meta := proposed[tx.Spec.Partition]
		encoded, err := json.Marshal(checkpoint{NextOffsets: meta.NextOffsets, StreamTime: meta.StreamTime})
		if err != nil {
			return b.abort(ctx, err)
		}
		appendRow(kgo.Record{Topic: b.config.ChangelogTopic, Partition: tx.Spec.Partition, Key: encodeKey(metadataKind, tx.Spec, "", ""), Value: encoded, Timestamp: now})
	}
	for partition, entry := range b.entries {
		if entry.barrier != nil {
			appendRow(kgo.Record{Topic: b.config.ChangelogTopic, Partition: partition, Key: entry.barrier, Value: nil, Timestamp: now})
		}
	}
	if len(rows) > 0 {
		if err := produceAll(ctx, b.session, rows); err != nil {
			return b.abort(ctx, fenceError(err))
		}
	}
	if b.tracker.current.Load() != b.assignment {
		return b.abort(ctx, errGroupRebalanced)
	}
	committed, err := b.session.End(ctx, kgo.TryCommit)
	if err != nil {
		return fenceError(err)
	}
	if !committed {
		return errGroupRebalanced
	}
	for partition, checkpoint := range proposed {
		entry := b.entries[partition]
		entry.checkpoint = checkpoint
		b.entries[partition] = entry
	}
	for partition, entry := range b.entries {
		entry.barrier = nil
		b.entries[partition] = entry
	}
	return nil
}
