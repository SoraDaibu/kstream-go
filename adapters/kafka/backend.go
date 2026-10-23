// Package kafka provides a Kafka transaction/changelog backend for explicitly
// assigned partition tasks. Consumer-group assignment is a separate future layer.
package kafka

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/SoraDaibu/kstream-go/execution"
	"github.com/SoraDaibu/kstream-go/record"
	"github.com/SoraDaibu/kstream-go/state"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers []string
	// ChangelogTopic must be provisioned with cleanup.policy=compact, durable
	// replication, and enough partitions for the assigned task partition numbers.
	ChangelogTopic string
	// Options supplies authentication/TLS and tuning. Internal transaction IDs,
	// isolation levels and changelog partitioning are enforced after these options.
	Options           []kgo.Opt
	OutputPartitioner kgo.Partitioner
}
type Backend struct {
	mu                     sync.Mutex
	config                 Config
	producer               *kgo.Client
	spec                   execution.Spec
	opened, failed, closed bool
	generation             uint64
	next                   map[string]int64
	barrier                []byte
}

var _ execution.Backend = (*Backend)(nil)

type checkpoint struct {
	NextOffsets map[string]int64 `json:"next_offsets"`
	StreamTime  int64            `json:"stream_time"`
}

func New(config Config) (*Backend, error) {
	if len(config.Brokers) == 0 || config.ChangelogTopic == "" {
		return nil, errors.New("brokers and changelog topic are required")
	}
	config.Brokers = append([]string(nil), config.Brokers...)
	config.Options = append([]kgo.Opt(nil), config.Options...)
	return &Backend{config: config}, nil
}
func (b *Backend) options(extra ...kgo.Opt) []kgo.Opt {
	opts := append([]kgo.Opt(nil), b.config.Options...)
	opts = append(opts, kgo.SeedBrokers(b.config.Brokers...))
	return append(opts, extra...)
}
func cloneOffsets(source map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(source))
	for k, v := range source {
		out[k] = v
	}
	return out
}
func transactionID(s execution.Spec) string {
	sum := sha256.Sum256(encodeKey(metadataKind, s, "", ""))
	return "kstream-" + hex.EncodeToString(sum[:])
}
func fenceError(err error) error {
	if errors.Is(err, kerr.ProducerFenced) || errors.Is(err, kerr.InvalidProducerEpoch) {
		return fmt.Errorf("%w: %v", execution.ErrFenced, err)
	}
	return err
}

type topicPartitioner struct {
	changelog string
	output    kgo.Partitioner
}

func (p topicPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	if topic == p.changelog {
		return kgo.ManualPartitioner().ForTopic(topic)
	}
	return p.output.ForTopic(topic)
}

func (b *Backend) Open(ctx context.Context, s execution.Spec) (uint64, execution.Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s.ApplicationID == "" || s.ID == "" || s.Partition < 0 {
		return 0, execution.Snapshot{}, errors.New("invalid Kafka task identity")
	}
	if b.closed {
		return 0, execution.Snapshot{}, errors.New("Kafka backend closed")
	}
	if b.opened && b.spec != s {
		return 0, execution.Snapshot{}, errors.New("Kafka backend belongs to a different task")
	}
	if b.producer != nil {
		b.producer.Close()
		b.producer = nil
	}
	b.failed = true
	output := b.config.OutputPartitioner
	if output == nil {
		output = kgo.UniformBytesPartitioner(64<<10, true, true, nil)
	}
	producer, err := kgo.NewClient(b.options(kgo.TransactionalID(transactionID(s)), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordPartitioner(topicPartitioner{b.config.ChangelogTopic, output}))...)
	if err != nil {
		return 0, execution.Snapshot{}, err
	}
	success := false
	defer func() {
		if !success {
			producer.Close()
		}
	}()
	token := make([]byte, 16)
	if _, err = rand.Read(token); err != nil {
		return 0, execution.Snapshot{}, err
	}
	marker := &kgo.Record{Topic: b.config.ChangelogTopic, Partition: s.Partition, Key: encodeKey(barrierKind, s, "", string(token)), Value: []byte("restore-barrier"), Timestamp: time.Now()}
	// Producing a real transaction initializes the producer epoch and fences a
	// previous owner before restoring a read-committed prefix through this marker.
	if err = producer.BeginTransaction(); err != nil {
		return 0, execution.Snapshot{}, fenceError(err)
	}
	if err = producer.ProduceSync(ctx, marker).FirstErr(); err != nil {
		_ = producer.EndTransaction(ctx, kgo.TryAbort)
		return 0, execution.Snapshot{}, fenceError(err)
	}
	if err = producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return 0, execution.Snapshot{}, fenceError(err)
	}
	snapshot, err := b.restore(ctx, s, marker.Offset)
	if err != nil {
		return 0, execution.Snapshot{}, err
	}
	b.producer = producer
	b.spec = s
	b.opened = true
	b.failed = false
	b.generation++
	b.next = cloneOffsets(snapshot.NextOffsets)
	b.barrier = record.CloneBytes(marker.Key)
	success = true
	return b.generation, snapshot, nil
}
func (b *Backend) restore(ctx context.Context, s execution.Spec, end int64) (execution.Snapshot, error) {
	reader, err := kgo.NewClient(b.options(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{b.config.ChangelogTopic: {s.Partition: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))...)
	if err != nil {
		return execution.Snapshot{}, err
	}
	defer reader.Close()
	snapshot := execution.Snapshot{State: make(state.Image), NextOffsets: make(map[string]int64), StreamTime: -1}
	for {
		fetches := reader.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			return execution.Snapshot{}, errs[0].Err
		}
		for _, r := range fetches.Records() {
			key, err := decodeKey(r.Key)
			if err != nil {
				return execution.Snapshot{}, err
			}
			if key.application == s.ApplicationID && key.task == s.ID && key.partition == s.Partition {
				switch key.kind {
				case stateKind:
					snapshot.State.Apply(state.Mutation{Store: key.store, Key: key.key, Value: r.Value})
				case metadataKind:
					if r.Value == nil {
						return execution.Snapshot{}, errors.New("checkpoint metadata was deleted")
					}
					var meta checkpoint
					if err = json.Unmarshal(r.Value, &meta); err != nil {
						return execution.Snapshot{}, err
					}
					if meta.StreamTime < -1 {
						return execution.Snapshot{}, errors.New("invalid checkpoint stream-time")
					}
					for topic, offset := range meta.NextOffsets {
						if topic == "" || offset < 0 {
							return execution.Snapshot{}, errors.New("invalid checkpoint offsets")
						}
					}
					snapshot.NextOffsets = cloneOffsets(meta.NextOffsets)
					snapshot.StreamTime = meta.StreamTime
				}
			}
			if r.Offset == end {
				return snapshot, nil
			}
			if r.Offset > end {
				return execution.Snapshot{}, errors.New("restore barrier is missing from changelog")
			}
		}
		if err = ctx.Err(); err != nil {
			return execution.Snapshot{}, err
		}
	}
}

func (b *Backend) Commit(ctx context.Context, s execution.Spec, generation uint64, tx execution.Transaction) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.failed || b.producer == nil {
		return execution.ErrPoisoned
	}
	if s != b.spec || generation != b.generation {
		return execution.ErrFenced
	}
	next := cloneOffsets(b.next)
	for topic, offset := range tx.NextOffsets {
		next[topic] = offset
	}
	meta, err := json.Marshal(checkpoint{next, tx.StreamTime})
	if err != nil {
		return err
	}
	rows := make([]*kgo.Record, 0, len(tx.Mutations)+len(tx.Outputs)+2)
	now := time.Now()
	for _, m := range tx.Mutations {
		rows = append(rows, &kgo.Record{Topic: b.config.ChangelogTopic, Partition: s.Partition, Key: encodeKey(stateKind, s, m.Store, m.Key), Value: m.Value, Timestamp: now})
	}
	for _, r := range tx.Outputs {
		if r.Topic == b.config.ChangelogTopic {
			return errors.New("user output cannot target the internal changelog")
		}
		rows = append(rows, &kgo.Record{Topic: r.Topic, Partition: r.Partition, Key: r.Key, Value: r.Value, Timestamp: time.UnixMilli(r.Timestamp)})
	}
	rows = append(rows, &kgo.Record{Topic: b.config.ChangelogTopic, Partition: s.Partition, Key: encodeKey(metadataKind, s, "", ""), Value: meta, Timestamp: now})
	if b.barrier != nil {
		rows = append(rows, &kgo.Record{Topic: b.config.ChangelogTopic, Partition: s.Partition, Key: b.barrier, Value: nil, Timestamp: now})
	}
	if err = b.producer.BeginTransaction(); err != nil {
		b.failed = true
		return fenceError(err)
	}
	if err = b.producer.ProduceSync(ctx, rows...).FirstErr(); err != nil {
		b.failed = true
		abortErr := b.producer.EndTransaction(ctx, kgo.TryAbort)
		return errors.Join(fenceError(err), abortErr)
	}
	if err = b.producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		b.failed = true
		return fenceError(err)
	}
	b.next = next
	b.barrier = nil
	return nil
}

// NextOffsets returns a committed checkpoint copy for explicitly assigned input.
func (b *Backend) NextOffsets() map[string]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneOffsets(b.next)
}
func (b *Backend) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	if b.producer != nil {
		b.producer.Close()
		b.producer = nil
	}
}
