# ADR 0017: Materialized KTable transforms

Status: Design before implementation; independent oracle gated.

Add KTable.Filter/FilterNot and MapTableValues as a type-changing package
function. Each result has an explicit store name and deterministic value codec,
so it remains a KTable usable for ToStream and current-value lookup joins.
Callbacks receive a read-only key and non-null value. Nullable map results use
(value, null); rejected filter updates become deletes. Input tombstones bypass
callbacks and delete the derived row. Null keys skip at the source boundary.

Use one typed task-local transform/materialization handler per result. Stage
result mutations before forwarding updates synchronously. Do not reuse stream
filter semantics, which would leave stale derived rows when an update no longer
matches. Do not route tombstones through stream MapValues, which permits null
revival. Preserve input timestamps and key partitioning. Source and derived
stores participate in the same backend transaction and restore together.

Implement from the public KTable contract, not Java internals. The rolling 3.9
Javadoc currently identifies 3.9.2; executable reference remains pinned to 3.9.1:
https://kafka.apache.org/39/javadoc/org/apache/kafka/streams/kstream/KTable.html
Independent traces must resolve exact repeated-delete/filtered-update emission
and chained lookup behavior. Include out-of-order input, nullable results,
empty/zero values, source replays and restoration after abort. Caching, table
retraction aggregates and versioned stores remain separate feature gates.
