# Kafka Streams feature inventory

Initial reference: Kafka Streams 3.9.1. Status: **NOT FEATURE COMPLETE**.
"Partial" means a tested subset exists, not family-wide equivalence. Each family
needs independent semantics, recovery and integration coverage before completion.

| Layer/family | Current status | Required work and evidence |
|---|---|---|
| Source/sink codecs, nulls, timestamps | Partial | Headers/metadata, timestamp extractors, serde failure policies, custom partitioners |
| KStream filter/map | Partial: filter/filterNot, map/mapValues, flatMap/flatMapValues, selectKey, merge, peek/foreach | Split/branch, print, richer metadata/error policies; broader reference coverage |
| KStream aggregation | Partial | groupBy/groupByKey exchange plans, aggregate, reduce, count and timestamp propagation |
| KTable | Partial | filter/mapValues, table aggregations/retractions, toStream/selectKey, caching and suppression |
| Stream-table joins | Partial | left joins, versioned table lookup, co-partitioning validation |
| Stream-stream joins | Missing | inner/left/outer, event-time bounds, grace, delayed unmatched results, duplicates and retention |
| Table-table joins | Missing | inner/left/outer, old/new values, tombstones and updates |
| Foreign-key table joins | Missing | subscription/repartition protocol, old-key removal, recovery |
| GlobalKTable | Missing | replicated global state, stream joins, bootstrap and fault tolerance |
| Windows | Partial | hopping, sliding, session, grace/retention, emit strategies, suppression and late records |
| Processor API | Partial | cross-type processors, record metadata/headers, punctuation, schedules, lifecycle and stores |
| Logical topology/compilation | Partial | exchange subtopologies, exchange validation, safe rewrites and optimization evidence |
| Partition runtime | Partial | backpressure, live fairness, pause/resume, idle partitions |
| Kafka adapter | Partial: explicit input, transactions, fencing, changelogs | Consumer groups, assignment/rebalance, authentication/TLS failure coverage and production lifecycle |
| Repartition | Rejected explicitly | broker exchanges, key serde/partitioner agreement, partition counts and recovery |
| Changelogs/restore | Partial: Kafka read-committed prefix restore and compactable full checkpoints | Forced-compaction validation, snapshot acceleration, restore callbacks and standby tasks |
| Exactly-once | Partial: Kafka atomic output/changelog/input checkpoints for fixed assignment | Consumer-group offsets/EOS v2, real ambiguous-commit fault injection, kill/rebalance tests |
| Persistent state | Partial: shared Pebble key-value namespaces, atomic batches, owned reads, explicit sync writes | streamed restore/cache checkpoint reuse, window/session/versioned stores, range queries, snapshots and retention |
| Interactive queries | Missing | task ownership discovery, local queries, consistency bounds, metadata and routing |
| Runtime control/observability | Missing | lifecycle, error policies, metrics, lag, health and shutdown |
| Testing | Partial | all families in differential oracle, property fuzzing, broker/crash/rebalance tests |
| Performance | Unqualified | matched Kafka Streams workload suite; see qualification.md |

Full parity is a substantial implementation program. This table is deliberately
not converted to a completion percentage: families differ greatly in difficulty.
