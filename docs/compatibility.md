# Compatibility matrix

Reference: Apache Kafka Streams 3.9.1. No production compatibility certification.
All changes to this table need executable evidence. The [validation report](validation.md)
links the passing Go/race suite and all five differential scenarios.

| Concept | v0 behavior | Evidence / remaining work |
|---|---|---|
| KStream filter/map | Typed synchronous callbacks; preserves timestamps/null flags | Go tests; Java oracle filter trace |
| KTable | Upsert, null-key skip, tombstone delete; ToStream emits input updates | Go tests; Java oracle table trace |
| Grouped count | Null keys/values skipped, emits each count update | Go tests; Java oracle count trace |
| Stream-table inner join | Stream-triggered current-value lookup, nulls skipped | Go tests; Java oracle join trace |
| Event time | Operator-local max timestamp for windows; task clock for custom processors | Go boundary/isolation tests; Java oracle |
| Tumbling count | [start,end), grace boundary, late drop, closed-window eviction; null values count when key is present | Go tests; Java oracle window trace |
| Processor API | Factories instantiate per task; explicit key-change declaration | Go tests |
| Partition tasks | Single partition number, task-local state | Isolation and race tests |
| Changelog/recovery/EOS | Atomic in-memory model with fencing, offsets, staged state | Failure tests; no Kafka adapter |
| Repartition | Logical explicit node; Build fails until Kafka exchange exists | Rejection tests |
| Other joins/windows | Not implemented | Do not infer compatibility |
| Caching/suppression | Disabled/absent; every update emitted | Deliberate difference, oracle cache disabled |
| Negative timestamps | Rejected | Deliberate restriction tested |
| Assignment/fault tolerance | Caller-owned task lifecycle; no rebalance protocol | Kafka adapter release gate |

Nulls use explicit flags, not Go zero values. Keys used in stores are encoded with
an explicit deterministic codec. Users must select codecs with stable encodings
and Kafka-compatible partitioning before implementing a distributed deployment.
