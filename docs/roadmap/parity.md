# Kafka Streams feature inventory

Initial reference: Kafka Streams 3.9.1. Status: **NOT FEATURE COMPLETE**.
"Partial" means a tested subset exists, not family-wide equivalence. Each family
needs independent semantics, recovery and integration coverage before completion.

| Layer/family | Current status | Required work and evidence |
|---|---|---|
| Source/sink codecs, nulls, timestamps | Partial: codecs/null flags, timestamps and processor source metadata | Headers, timestamp extractors, serde failure policies, custom partitioners |
| KStream filter/map | Partial: filter/filterNot, map/mapValues, flatMap/flatMapValues, selectKey, merge, peek/foreach | Split/branch, print, richer metadata/error policies; broader reference coverage |
| KStream aggregation | Partial: groupByKey, count, reduce, typed aggregate and timestamp propagation | groupBy broker exchange plans, caching, window aggregates and broader reference coverage |
| KTable | Partial: source, filter/filterNot, nullable mapValues, ToStream and lookup joins | table aggregations/retractions, toStream/selectKey, caching and suppression |
| Stream-table joins | Partial: inner/left current-value lookup and nullable results | versioned table lookup, co-partitioning validation and broker scheduling qualification |
| Stream-stream joins | Missing | inner/left/outer, event-time bounds, grace, delayed unmatched results, duplicates and retention |
| Table-table joins | Missing | inner/left/outer, old/new values, tombstones and updates |
| Foreign-key table joins | Missing | subscription/repartition protocol, old-key removal, recovery |
| GlobalKTable | Missing | replicated global state, stream joins, bootstrap and fault tolerance |
| Windows | Partial: tumbling/hopping counts and per-window grace | sliding, session, window aggregates, configurable retention, emit strategies, suppression and broader late-record qualification |
| Processor API | Partial: same/cross-type task factories, staged stores and source-record metadata | fixed-key processors, headers, punctuation, schedules, lifecycle and richer stores |
| Logical topology/compilation | Partial | exchange subtopologies, exchange validation, safe rewrites and optimization evidence |
| Partition runtime | Partial: task ownership, bounded batches, atomic groups and serial Kafka worker | cross-topic timestamp scheduling, idle controls, live fairness, pause/resume and broader backpressure |
| Kafka adapter | Partial: fixed input and bounded group worker, transactions, fencing, changelogs | Cooperative/heterogeneous assignment, authentication/TLS failure coverage and broader production lifecycle |
| Repartition | Rejected explicitly | broker exchanges, key serde/partitioner agreement, partition counts and recovery |
| Changelogs/restore | Partial: Kafka read-committed prefix restore, bounded mutation batches and compactable full checkpoints | Forced-compaction validation, orphan-marker GC, local checkpoint reuse, restore lifecycle callbacks and standby tasks |
| Exactly-once | Partial: fixed checkpoints and group-offset/state/output transactions | Broader kill/ambiguous-network/timeout/compaction and rebalance qualification; do not infer full EOS v2 parity |
| Persistent state | Partial: shared Pebble key-value namespaces, streamed cache loads, atomic batches, owned reads, explicit sync writes | local checkpoint reuse, large-state qualification, window/session/versioned stores, range queries, snapshots and retention |
| Interactive queries | Missing | task ownership discovery, local queries, consistency bounds, metadata and routing |
| Runtime control/observability | Partial: context worker lifecycle, explicit task close and committed batch observations | metrics registry, lag, health, richer lifecycle/error policies and shutdown fault qualification |
| Testing | Partial | all families in differential oracle, property fuzzing, broker/crash/rebalance tests |
| Performance | Unqualified | matched Kafka Streams workload suite; see qualification.md |

Full parity is a substantial implementation program. This table is deliberately
not converted to a completion percentage: families differ greatly in difficulty.
