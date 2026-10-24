# Kafka transaction backend — experimental fixed assignment

This page describes Backend and Input for explicit assignment.
[RunGroup](GROUPS.md) adds a separate experimental consumer-group worker with
atomic group offsets, state and output and a bounded task owner.

This adapter uses franz-go's public client APIs. It persists state changes,
serialized output and a **full input-position checkpoint** in one Kafka producer
transaction. State keys are scoped by application, task, partition, store and key.
Offsets for every input topic are retained in each checkpoint so topic compaction
cannot erase an unchanged input's position.

Open initializes a stable task transactional ID by committing a unique restore
barrier, fencing an old producer before reading the committed log prefix through
that barrier. Unique barrier keys remain reachable during concurrent compaction;
the task removes its barrier in its first successful data transaction. Restore
applies state tombstones and the latest complete checkpoint. Ambiguous commit
errors poison the task; a fresh incarnation restores the broker's committed state.

Task construction uses OpenInto to replay committed state directly into the
task store, without a complete intermediate state image. RestoreBatchSize
defaults to 256 and bounds synchronous mutation batches; RestoreFetchBytes
defaults to an 8 MiB fetch target with one fetch in flight/buffered. Kafka may
return an oversized record batch despite that target, and compressed batches
expand in memory. Broker message/batch limits still matter. Open remains an
explicit image-collecting API for legacy callers. A sink failure leaves the
backend unusable until a fresh successful open; no partial task is published.

The changelog topic must be provisioned with `cleanup.policy=compact`, durable
replication and enough partitions. Outputs use Kafka-compatible keyed hashing
through franz-go; changelogs always use the explicit task partition. Kafka/TLS
and authentication options stay inside this adapter. Applications must close
backend and input clients during task shutdown.

`Backend.Input` consumes explicitly assigned source partitions from the committed
checkpoint using read-committed isolation. Feed each returned bounded slice to
`Task.ProcessBatch` before polling again. Recreate both input and task after a
input, processing or commit error. The task ID and application ID must remain stable across
restarts, and source ownership must be externally controlled.

Transaction production waits for every record's completion before committing or
aborting. The real client's public Produce API uses one completion callback and
retains only the first error, avoiding a result list proportional to the output
batch. Fresh record arrays/payload lifetimes are unchanged. A custom injected
session exposing only ProduceSync keeps that compatibility path. An optional
Produce implementation must complete each submitted callback exactly once,
including cancellation/failure; concurrent callbacks are supported.

## Boundaries

Fixed-assignment input positions are stored atomically in the application's
compacted changelog. Consumer-group offset transactions and eager assignment are
provided by the separate RunGroup API. Standby tasks, local checkpoint
reuse/migration, restore-marker garbage collection, automatic topic provisioning
and distributed repartition subtopologies remain separate work. No production qualification is
claimed from the single-broker integration fixture.
Optional Pebble task stores are covered by the integration suite, but this is
not yet a complete Kafka Streams state subsystem.

The integration workflow provisions a Kafka 3.9.1 broker and exercises restart
recovery, replay suppression, aborted-record isolation, multi-topic checkpoints,
tombstones/empty values, stale-producer fencing, partition identity and explicit
input resumption. Tests require `KSTREAM_KAFKA_BROKERS` and the three documented
`kstream-test-*` topics; ordinary unit tests skip broker-specific cases otherwise.
