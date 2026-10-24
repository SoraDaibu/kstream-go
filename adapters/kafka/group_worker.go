package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/twmb/franz-go/pkg/kgo"
)

// GroupConfig configures one serial worker that schedules its assigned tasks.
// Every member must use the same topics/topology and a unique stable InstanceID.
type GroupConfig struct {
	Kafka                     Config
	ApplicationID, InstanceID string
	Topics                    []string
	BatchSize                 int
	FetchBytes                int32
	PollInterval              time.Duration
	RestoreTimeout            time.Duration
}

// TaskFactory binds a topology and optional store factory at the composition root.
// The returned task must use the supplied spec and backend. Tasks and their stores
// are owned/closed by RunGroup; shared engines remain owned by the caller.
// The context bounds initialization and is canceled when the factory returns.
type TaskFactory func(context.Context, execution.Spec, execution.Backend) (*execution.Task, error)

func normalizeGroup(config GroupConfig) (GroupConfig, error) {
	if config.ApplicationID == "" || config.InstanceID == "" || len(config.Topics) == 0 {
		return config, errors.New("application ID, stable unique instance ID and source topics required")
	}
	validated, err := New(config.Kafka)
	if err != nil {
		return config, err
	}
	config.Kafka = validated.config
	seen := make(map[string]bool, len(config.Topics))
	config.Topics = append([]string(nil), config.Topics...)
	for _, topic := range config.Topics {
		if topic == "" || topic == config.Kafka.ChangelogTopic || seen[topic] {
			return config, errors.New("source topics must be unique, nonempty and separate from changelog")
		}
		seen[topic] = true
	}
	if config.BatchSize == 0 {
		config.BatchSize = 256
	}
	if config.FetchBytes == 0 {
		config.FetchBytes = 8 << 20
	}
	if config.PollInterval == 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if config.RestoreTimeout == 0 {
		config.RestoreTimeout = time.Minute
	}
	if config.BatchSize < 1 || config.BatchSize > 65536 || config.FetchBytes < 1024 || config.PollInterval < time.Millisecond || config.RestoreTimeout < time.Millisecond {
		return config, errors.New("invalid group batch, fetch or timeout bounds")
	}
	return config, nil
}
func workerTransactionID(application, instance string) string {
	sum := sha256.Sum256(encodeKey(metadataKind, execution.Spec{ApplicationID: application, ID: instance}, "group-worker", ""))
	return "kstream-group-" + hex.EncodeToString(sum[:])
}
func closeGroupTasks(tasks map[int32]*execution.Task) error {
	var err error
	for partition, task := range tasks {
		err = errors.Join(err, task.Close())
		delete(tasks, partition)
	}
	return err
}

// Initialize the stable worker producer epoch before any poll or task restore.
// Aborting this control record fences an abandoned worker transaction without
// advancing consumer-group offsets or publishing state/output.
func fenceWorker(ctx context.Context, session groupTransactor, config GroupConfig) error {
	if err := session.Begin(); err != nil {
		return fenceError(err)
	}
	spec := execution.Spec{ApplicationID: config.ApplicationID, ID: "$worker$/" + config.InstanceID}
	control := &kgo.Record{Topic: config.Kafka.ChangelogTopic, Partition: 0, Key: encodeKey(barrierKind, spec, "", "bootstrap"), Value: []byte("fence-worker"), Timestamp: time.Now()}
	produceErr := session.ProduceSync(ctx, control).FirstErr()
	_, abortErr := session.End(ctx, kgo.TryAbort)
	return errors.Join(fenceError(produceErr), fenceError(abortErr))
}

// RunGroup processes a Kafka consumer group using one bounded serial worker.
// It atomically commits the complete poll's group offsets, state and output.
// Rebalances discard task incarnations and resume committed positions. Other
// errors terminate the worker; restart fresh after resolving the cause. Context
// cancellation returns ctx.Err after releasing task stores and leaving the group.
// This experimental implementation uses eager range assignment and requires
// equal, fixed source partition counts. It is not full Kafka Streams parity.
func RunGroup(ctx context.Context, config GroupConfig, factory TaskFactory) (err error) {
	if factory == nil {
		return errors.New("task factory required")
	}
	config, err = normalizeGroup(config)
	if err != nil {
		return err
	}
	tracker := &assignmentTracker{topics: config.Topics}
	output := config.Kafka.OutputPartitioner
	if output == nil {
		output = kgo.UniformBytesPartitioner(64<<10, true, true, nil)
	}
	opts := append([]kgo.Opt(nil), config.Kafka.Options...)
	opts = append(opts, kgo.SeedBrokers(config.Kafka.Brokers...), kgo.ConsumerGroup(config.ApplicationID), kgo.ConsumeTopics(config.Topics...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.TransactionalID(workerTransactionID(config.ApplicationID, config.InstanceID)), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableAutoCommit(), kgo.RequireStableFetchOffsets(), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.Balancers(kgo.RangeBalancer()), kgo.OnPartitionsAssigned(tracker.assigned), kgo.OnPartitionsRevoked(tracker.revoked), kgo.OnPartitionsLost(tracker.revoked),
		kgo.RecordPartitioner(topicPartitioner{config.Kafka.ChangelogTopic, output}), kgo.FetchMaxBytes(config.FetchBytes), kgo.FetchMaxPartitionBytes(config.FetchBytes), kgo.MaxConcurrentFetches(1))
	session, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		return err
	}
	defer session.Close()
	if blocked, _ := session.Client().OptValue(kgo.BlockRebalanceOnPoll).(bool); blocked {
		return errors.New("RunGroup requires automatic rebalance callbacks; BlockRebalanceOnPoll is unsupported")
	}
	count, err := sourcePartitionCount(ctx, session.Client(), config.Topics, config.Kafka.ChangelogTopic)
	if err != nil {
		return err
	}
	if err := fenceWorker(ctx, session, config); err != nil {
		return err
	}
	backend := &groupBackend{config: config.Kafka, application: config.ApplicationID, session: session, tracker: tracker}
	group, err := execution.NewGroup(backend)
	if err != nil {
		return err
	}
	tasks := make(map[int32]*execution.Task)
	defer func() { err = errors.Join(err, closeGroupTasks(tasks)) }()
	reset := func(assignment *groupAssignment) error {
		if err := closeGroupTasks(tasks); err != nil {
			return err
		}
		backend.reset(assignment)
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		assignment := tracker.current.Load()
		if assignment != backend.assignment {
			if err := reset(assignment); err != nil {
				return err
			}
		}
		if assignment != nil && assignment.err != nil {
			return assignment.err
		}
		pollCtx, cancel := context.WithTimeout(ctx, config.PollInterval)
		fetches := session.PollRecords(pollCtx, config.BatchSize)
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		assignment = tracker.current.Load()
		if assignment != backend.assignment {
			if err := reset(assignment); err != nil {
				return err
			}
		}
		if assignment != nil && assignment.err != nil {
			return assignment.err
		}
		rows := fetches.Records()
		if failures := fetches.Errors(); len(failures) > 0 {
			deadline := true
			for _, failure := range failures {
				if !errors.Is(failure.Err, context.DeadlineExceeded) {
					deadline = false
					err = failure.Err
					break
				}
			}
			if len(rows) > 0 {
				if abortErr := backend.abortPoll(ctx); abortErr != nil {
					return abortErr
				}
			}
			if deadline {
				continue
			}
			return err
		}
		if len(rows) == 0 {
			continue
		}
		if assignment == nil || !assignment.valid {
			if err := backend.abortPoll(ctx); err != nil {
				return err
			}
			continue
		}
		for partition := range assignment.partitions {
			if partition >= count {
				return errors.New("source partition count changed while worker was running")
			}
		}
		batches := make([]execution.TaskBatch, 0, len(assignment.partitions))
		positions := make(map[int32]int, len(assignment.partitions))
		backend.pollNext = make(map[int32]map[string]int64)
		ownershipChanged := false
		for _, row := range rows {
			if !assignment.partitions[row.Partition] {
				ownershipChanged = true
				break
			}
			task := tasks[row.Partition]
			if task == nil {
				restoreCtx, stop := context.WithTimeout(ctx, config.RestoreTimeout)
				task, err = factory(restoreCtx, execution.Spec{ApplicationID: config.ApplicationID, ID: "0", Partition: row.Partition}, group)
				stop()
				if err != nil {
					if errors.Is(err, errGroupRebalanced) {
						ownershipChanged = true
						break
					}
					return err
				}
				if task == nil {
					return errors.New("task factory returned nil")
				}
				tasks[row.Partition] = task
			}
			index, ok := positions[row.Partition]
			if !ok {
				index = len(batches)
				positions[row.Partition] = index
				batches = append(batches, execution.TaskBatch{Task: task})
			}
			batches[index].Records = append(batches[index].Records, record.Bytes{Topic: row.Topic, Partition: row.Partition, Offset: row.Offset, Timestamp: row.Timestamp.UnixMilli(), Key: row.Key, Value: row.Value})
			if backend.pollNext[row.Partition] == nil {
				backend.pollNext[row.Partition] = make(map[string]int64)
			}
			backend.pollNext[row.Partition][row.Topic] = row.Offset + 1
		}
		if ownershipChanged || tracker.current.Load() != assignment {
			if err := backend.abortPoll(ctx); err != nil {
				return err
			}
			if err := reset(tracker.current.Load()); err != nil {
				return err
			}
			continue
		}
		if err := group.Process(ctx, batches); err != nil {
			if !errors.Is(err, errGroupRebalanced) {
				return fmt.Errorf("Kafka group processing: %w", err)
			}
			if err := reset(tracker.current.Load()); err != nil {
				return err
			}
		}
	}
}
