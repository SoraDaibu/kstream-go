# Kafka Streams feature inventory

Initial reference: Kafka Streams 3.9.1. Status: **NOT FEATURE COMPLETE**.
"Partial" means a tested subset exists, not family-wide equivalence. Each family
needs independent semantics, recovery and integration coverage before completion.

| Layer/family | Current status | Required work and evidence |
|---|---|---|
| Source/sink codecs, nulls, timestamps | Partial | Headers/metadata, timestamp extractors, serde failure policies, custom partitioners |
| KStream filter/map | Partial | filterNot, mapValues, flatMap, flatMapValues, selectKey, merge, split/branch, peek/foreach, print |
| KStream aggregation | Partial | groupBy/groupByKey exchange plans, aggregate, reduce, count and timestamp propagation |
| KTable | Partial | filter/mapValues, table aggregations/retractions, toStream/selectKey, caching and suppression |
| Stream-table joins | Partial | left joins, versioned table lookup, co-partitioning validation |
| Stream-stream joins | Missing | inner/left/outer, event-time bounds, grace, delayed unmatched results, duplicates and retention |
| Table-table joins | Missing | inner/left/outer, old/new values, tombstones and updates |
| Foreign-key table joins | Missing | subscription/repartition protocol, old-key removal, recovery |
| GlobalKTable | Missing | replicated global state, stream joins, bootstrap and fault tolerance |
| Windows | Partial | hopping, sliding, session, grace/retention, emit strategies, suppression and late records |
| Processor API | Partial | cross-type processors, record metadata/headers, punctuation, schedules, lifecycle and stores |
| Logical topology/compilation | Partial | exchange subtopologies, graph validation, naming, describe, safe optimizations |
| Partition runtime | Partial | transaction batches, ordering, backpressure, fairness, pause/resume, idle partitions |
| Kafka adapter | Missing | fetch/produce, groups, assignment, rebalance fencing, authentication and broker errors |
| Repartition | Rejected explicitly | broker exchanges, key serde/partitioner agreement, partition counts and recovery |
| Changelogs/restore | In-memory model | Kafka compaction, read-committed restore, checkpoints, restore callbacks and standby tasks |
| Exactly-once | In-memory model | atomic output/changelog/group offsets, producer fencing, ambiguous commits, crash/rebalance injection |
| Persistent state | Missing | key-value/window/session/versioned stores, write batches, snapshots, retention, iterators |
| Interactive queries | Missing | task ownership discovery, local queries, consistency bounds, metadata and routing |
| Runtime control/observability | Missing | lifecycle, error policies, metrics, lag, health and shutdown |
| Testing | Partial | all families in differential oracle, property fuzzing, broker/crash/rebalance tests |
| Performance | Unqualified | matched Kafka Streams workload suite; see qualification.md |

Full parity is a substantial implementation program. This table is deliberately
not converted to a completion percentage: families differ greatly in difficulty.
