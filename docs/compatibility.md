# Compatibility matrix

Reference: Apache Kafka Streams 3.9.1. No production compatibility certification.
All changes to this table need executable evidence. The [validation report](validation.md)
links the passing Go/race suite and differential scenarios.

| Concept | v0 behavior | Evidence / remaining work |
|---|---|---|
| KStream transforms/merge | Typed synchronous callbacks; null-aware value mapping, streaming flat maps, key selection and shared merge graph | Go tests; Java filter/filterNot/mapValues/flatMap/flatMapValues/merge traces |
| Ordered stream branching | First matching predicate wins; unmatched drop or explicit catch-all; keys/nulls/timestamps preserved | Go ownership/recovery/rollback tests; independent Java split traces passed |
| KTable | Upsert, null-key skip, tombstone delete; ToStream emits input updates | Go tests; Java oracle table trace |
| KTable transforms | Materialized filter/filterNot and nullable type-changing MapTableValues; tombstones bypass callbacks, filters suppress absent-row deletes | Callback, zero/empty, failure/recovery tests; four Java traces passed |
| Grouped count | Null keys/values skipped, emits each count update | Go tests; Java oracle count trace |
| Grouped reduce/aggregate | Explicit nullable results, aggregate timestamps, delete/reinitialize, nullable initializer | Go tests; reference traces are required for every implementation change |
| Stream-table inner/left join | Stream-triggered current-value lookup; explicit absent-right and nullable-result flags; left join preserves null stream keys | Go null/empty/recovery tests; Java join, join-null and left-join traces passed |
| Event time | Operator-local max timestamp for windows; task clock for custom processors | Go boundary/isolation tests; Java oracle |
| Tumbling count | [start,end), grace boundary, late drop, closed-window eviction; null values count when key is present | Go tests; Java oracle window trace |
| Hopping count | Overlapping half-open windows, ascending start order, per-window grace, null values count | Membership/recovery/aborted-fanout tests; divisible/uneven advance Java traces passed |
| Windowed reduce | Tumbling/hopping typed reduce, first-value initialization, nullable result deletion, lifetime timestamps; null keys skip, null values advance clock and reach reducer with a flag | Go recovery/overlap/null/rollback tests; independent Java traces passed |
| Processor API | Same/cross-type factories instantiate per task; conservative key-change validation; DSL source metadata, raw runtime opt-in | Go callback/fanout/state/replay/rollback tests; Java metadata and cross-type traces passed |
| Partition tasks | Single partition number, task-local state | Isolation and race tests |
| Changelog/recovery/EOS | In-memory model, fixed checkpoints and experimental group-offset transactions | Model and broker recovery/fencing/aborted-record gates; broader group fault qualification remains open |
| Persistent local stores | Optional shared Pebble engine, task-owned key-value namespaces, atomic batches, synchronous write mode and streamed cache loads | Ownership/reopen/failure tests; local checkpoint reuse and specialized stores pending |
| Repartition | Logical explicit node; Build fails until Kafka exchange exists | Rejection tests |
| Other joins/windows | Not implemented | Do not infer compatibility |
| Caching/suppression | Disabled/absent; every update emitted | Deliberate difference, oracle cache disabled |
| Negative timestamps | Rejected | Deliberate restriction tested |
| Assignment/fault tolerance | Explicit assignment or experimental eager range group worker with co-location checks | Group restart/rebalance/kill gates; cooperative assignment, standby and broader failure qualification pending |

Nulls use explicit flags, not Go zero values. Keys used in stores are encoded with
an explicit deterministic codec. Users must select codecs with stable encodings
and Kafka-compatible partitioning before implementing a distributed deployment.
