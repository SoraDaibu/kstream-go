# ADR 0006: Kafka-backed fixed-assignment transaction adapter

Status: Experimental implementation; broker and production qualification gated.

The first real infrastructure adapter implements the existing task transaction
contract with franz-go. It adds no Kafka dependency to operator execution or DSL
code. A producer per explicitly owned task has a stable identity including its
partition. A committed restore barrier fences an old owner before read-committed
restoration. Barrier keys are unique per incarnation so compaction cannot replace
the exact marker before a restore reaches it.

Changelog store records use versioned, length-delimited scope keys and Kafka null
values for deletes. Each transaction includes its state writes, all user output,
and a complete input-topic offset map plus stream-time. Full maps are essential:
a compacted metadata key containing only changed-topic offsets would lose other
inputs after compaction. State/output/offset publication therefore shares one
Kafka commit decision. No local state becomes visible before acknowledgement.

This gives a fixed-assignment checkpoint protocol. It deliberately does not claim
consumer-group EOS v2: group assignment, offset metadata, rebalance coordination
and multi-task group commits require a separate coordinator. Do not hide that gap
by calling the adapter a complete Kafka Streams runtime. A future group adapter
must coordinate all polled partitions before committing group positions and must
not let one task commit offsets for records another task has not processed.

Kafka-backed checkpoint recovery is distinct from local store persistence. Local
state remains rebuildable memory in this cycle; durable/standby stores and faster
restoration are later capabilities. Production comparisons must match the actual
backend and processing guarantees, not compare this protocol with weaker settings.
