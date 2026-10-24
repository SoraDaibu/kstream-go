# Experimental Kafka group worker

RunGroup gives one bounded serial worker ownership of its Kafka consumer/producer
session and assigned partition tasks. It uses franz-go's public transaction
session to commit input group offsets, changelogs and output together. Each poll
is bounded by BatchSize; task callbacks run synchronously and local caches apply
only after the broker decision. Operators do not have their own goroutines.

```go
err := kafka.RunGroup(ctx, kafka.GroupConfig{
    Kafka: kafka.Config{Brokers: brokers, ChangelogTopic: "app-state"},
    ApplicationID: "app", InstanceID: "worker-a", Topics: []string{"events"},
    BatchSize: 256,
}, func(ctx context.Context, spec execution.Spec, backend execution.Backend) (*execution.Task, error) {
    return topology.NewTask(ctx, spec, backend,
        execution.WithStoreFactory(engine.Task))
})
```

The factory context bounds initialization and is canceled when the factory
returns. RunGroup owns and closes the returned tasks; the application owns the
engine. Use a separate engine directory per worker instance. Tasks within one
worker share one physical engine. Cross-worker cache-lease handoff on a shared
engine is not implemented; a duplicate namespace is rejected.

ApplicationID is the Kafka consumer group ID and must identify one topology.
Every member must subscribe to the same source topics and use compatible codecs.
InstanceID must be unique among active workers and stable across that worker's
restart. Reusing it fences an old producer. Changing it after a crash can delay
restoration behind an old transaction until that transaction resolves.

Before input polling or task restoration, initialize the stable worker producer
epoch, retrying explicitly retryable initialization errors within the setup
budget, then abort an internal control-record transaction. No input offsets are
committed during this fence. In particular, CONCURRENT_TRANSACTIONS can occur
while Kafka aborts a dead worker's epoch; it is retried before producing records.
Transaction-end errors remain fatal and may be ambiguous.

The first implementation requires equal, fixed source partition counts, enough
changelog partitions and eager range assignment. Runtime checks co-location and
that task sources match the subscription. All polled input must be covered by
proposed checkpoints before group offsets can advance. Sources start at the
beginning when the group has no committed offset. A rebalance-aborted decision
discards local task incarnations and restores committed state. Other errors stop
the worker; recover fresh after resolving the cause.

Kafka.Options supplies common transport/authentication configuration to every
adapter client. GroupConfig.Options applies only to the worker session, e.g.
group timeouts. Internal assignment, isolation, offset, producer and partitioner
policies are enforced afterward. BlockRebalanceOnPoll is rejected; the worker
allows automatic revocation and aborts affected transactions. PollInterval
defaults to 250ms so idle workers release revoked tasks promptly. RestoreTimeout
defaults to one minute and also bounds startup initialization. Set the worker's
group/poll timeouts large enough for restore and processing of an entire poll.
Callbacks must return within that budget; arbitrary Go callbacks are not preempted.

Optional OnBatch receives GroupBatchMetrics after a successful processing
transaction and all local applies. Records counts polled input (including a
checkpointed replay), Tasks counts participating tasks, and ProcessDuration
includes callbacks, broker commit and local apply. It excludes polling, restore
and the observer. No timer is sampled when the observer is nil. The observer
runs under the worker owner and must return promptly; monitoring effects are
outside the processing transaction. Bootstrap/restore transactions are not
reported as processing batches.

Provision source/output topics and a compacted changelog explicitly. The initial
broker reference is Kafka 3.9.1. The complete
[group-count example](../../examples/kafka-group-count/main.go) requires:

```sh
KAFKA_BROKERS=localhost:9092 KSTREAM_INSTANCE_ID=worker-a \
  STATE_DIR=./state-a go run ./examples/kafka-group-count
```

Defaults are application `kstream-group-count`, input `words`, output
`counts-out` and changelog `kstream-group-count-state`. Override with
KSTREAM_APPLICATION_ID, INPUT_TOPIC, OUTPUT_TOPIC and CHANGELOG_TOPIC. Start a
second member with a different instance ID and state directory.

This remains a tested subset, not full Kafka Streams fault tolerance or a
throughput qualification. Cooperative assignment, heterogeneous subtopologies,
cross-topic timestamp scheduling and idle controls, heterogeneous exchange stages,
standby replicas, checkpoint reuse, restore-marker garbage
collection after failed startup, and broader timeout/network/compaction failure
coverage remain open. Never mix fixed-assignment and group workers under the
same application identity. See [ADR 0012](../../docs/adr/0012-kafka-group-worker.md).
