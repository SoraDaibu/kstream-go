# ADR 0005: Topology compilation and operator optimization

Status: Accepted baseline; rewrites deferred until equivalence is demonstrated.

## Decision

Keep logical topology metadata separate from typed executable factories and
infrastructure. Build validates names, parent references, source uniqueness,
state-store ownership, and unsupported exchange boundaries, then freezes a
snapshot. A key-changing processor propagates a repartition-required property;
Filter and To may follow it, but grouping and key-local joins require an exchange.
A Kafka adapter will compile each exchange to a sink/source subtopology with
partition counts and serializer/partitioner compatibility validated explicitly.

For now compile typed ports into synchronous chains once per task. This removes
operator scheduling overhead without changing observable ordering. Preserve
branch subscriber insertion order. Do not reorder user callbacks, filter across
stateful operations, eliminate tombstones, or coalesce table updates. Stateful
nodes are optimization barriers. Logical descriptions remain deterministic.

A proposed rewrite must show oracle equivalence for nulls, timestamps, errors,
branch ordering and state, then improve representative benchmarks. Future work:
linear-chain specialization, redundant serde removal at safe internal boundaries,
and batched state writes. Avoid reflection and per-record dispatch by graph IDs.

## Consequences

Go's compiler can inline some callbacks; no performance assumption is made about
Java optimizations. Branch dispatch retains a small function-slice loop. The
logical and physical plans may evolve independently. A fused/unfused differential
mode is required before adding any semantics-changing optimizer.

## Shared physical nodes

The internal/graph package memoizes compiled typed ports per task. A merge or
other shared downstream node must instantiate its processor factory exactly once,
even when multiple source roots reach it. A regression tests this using a
processor-local counter across two sources and independent task incarnations.
Bindings use dynamic keys/values only during setup; record dispatch remains a
synchronous typed handler chain with no per-record boxing or graph-map lookup.
Logical topology validation remains independent of this physical compilation.
