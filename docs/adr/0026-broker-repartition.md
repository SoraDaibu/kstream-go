# ADR 0026: Explicit broker repartition boundaries

Status: accepted for implementation; differential and broker validation required.

Implement KStream.Repartition as a transactional internal-topic sink plus a
separate consumed source, with no direct synchronous forwarding across the
boundary. The upstream task commits its state, exchange records and input
positions; downstream tasks consume only committed exchange records and commit
their own state/output/positions. Reuse the existing Kafka transaction, fencing,
recovery and group-offset protocols. No operator goroutines or channels are added.

Keep the minimal name-based API and derive the topic as
applicationID-name-repartition. Expose Topology.InputTopics(applicationID) and
Topology.RepartitionTopics(applicationID) for deployment wiring/provisioning.
Validate generated names and collisions before opening a task backend. Explicit
topic provisioning remains required, including matching partition counts and
delete retention for exchanges; automatic provisioning is separate work.

Compile each side into the existing typed ports and explicit codecs. The returned
stream is partitioned by its encoded key, so key-local operations become valid.
Preserve keyed value/null/timestamp data. The independent Kafka Streams 3.9.1
oracle rejected the initial null-key forwarding behavior; drop null keys before
writing the exchange, while committing their source positions. Retain the
randomized differential scenario and direct null/empty regression test. Application IDs, exchange names and
codecs must remain stable across restarts. Generated topics cannot collide with
external sources or user sinks in the same topology.

Kafka adapters accept an explicit RepartitionTopics list and force Kafka keyed
hash routing for those topics even when ordinary output uses a custom partitioner.
An execution backend validation capability is required for exchange topologies
and checks configuration before task open; transaction groups forward it and
reject hidden/unsupported transports. Memory backends explicitly accept caller
transport: tests deliver committed exchange outputs to the selected downstream partition.

Retain the current equal-partition-count/eager co-location worker. One native
task owns compiled stages for its partition number, while broker exchanges still
separate delivery and transactions. ADR 0027 supersedes the initial shared
custom-processor-clock boundary with connected-stage clocks, without copying
Kafka's task hierarchy. Heterogeneous partition counts, independent stage
scheduling and custom internal partitioners remain unsupported.

Validate source/sink setup ownership, actual cross-partition grouping, null/empty
payloads, replay/recovery, aborted exchange visibility and real group offsets.
Add public Java repartition traces and a real Kafka test with a custom ordinary
partitioner, forced internal hash routing, restart and an aborted exchange record.
Do not infer global ordering across independent sources/stages or full deployment
compatibility from these covered cases.
