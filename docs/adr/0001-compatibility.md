# ADR 0001: Kafka Streams compatibility boundaries

Status: Accepted for the experimental core; production compatibility remains gated.

This records the initial v0 scope. Later ADRs and the current compatibility
matrix supersede its original unavailable-feature list. In particular, ADR 0026
adds explicit broker exchanges with manual provisioning/equal-count workers and
ADR 0027 adds transactional connected-stage custom processor clocks.

## Context

Apache Kafka Streams is the semantic reference, not an implementation template.
This is an independent Go implementation; no Java source, package hierarchy,
threading model, or class hierarchy is ported. Apache Kafka and Kafka Streams are
Apache Software Foundation trademarks. This project is independent of the ASF.

## Decision

Preserve KStream record semantics, KTable keyed upserts and tombstones, logical
topologies, a DSL over typed processors, partition tasks, task-local stores,
changelog recovery, explicit repartition boundaries, and event-time aggregation.
Pin the initial differential oracle to Kafka Streams 3.9.1. A version change
requires rerunning the oracle and reviewing changed behavior.

A null key is represented by Record.NullKey; a null value by Record.NullValue.
Go zero values are ordinary data. Streams preserve tombstones through stateless
operators; tables ignore null keys and delete on tombstones. Stream-table inner
joins are triggered only by stream input and consult the current table value.
Tumbling windows are [start,end), use nonnegative Unix milliseconds, and reject
records when operator stream-time >= window-end + grace. Unwindowed count skips null keys and
values. Kafka Streams 3.9.1 windowed count skips null keys but counts null values
with a present key; this difference is pinned by the oracle and regression test.
Window stores evict closed windows; count emits updates, without caching
or suppression. Window stream-time advances on input with a non-null key admitted to that
operator, independently of unrelated sources and upstream filters. A separate
task clock is available to custom processors. Both clocks survive recovery;
the window clock is staged in the same transaction as its state.

Deliberate v0 restrictions: one partition per task, no assignment/rebalance
protocol, no versioned tables, no cache coalescing, no stream-stream or table-table
joins, no hopping/session windows, and no automatic distributed repartitioning.
Negative timestamps and mismatched input partitions fail. A key-changing
processor marks its output unpartitioned; stateful operations reject it until an
explicit repartition boundary. The prototype rejects Repartition at Build rather
than pretending local routing is a broker exchange. A raw Processor must declare
whether keys change. Named stores cannot be silently shared by operators.

Exactly-once is a *backend contract*, not a production guarantee. The memory
adapter models atomic output + changelog + next offsets and fences stale tasks.
No production Kafka adapter exists yet. Do not advertise broker-level EOS,
crash-safe local durability, or Kafka partition assignment until integration
and failure-injection gates pass.

## Validation and alternatives

Unit tests cover restrictions and boundaries. The Java TopologyTestDriver oracle
compares deterministic output traces for filters, counts, table deletes, joins,
and grace boundaries, with caching disabled. Seeded traces and regressions belong
in the differential suite. The in-memory adapter cannot validate broker failures,
producer fencing by Kafka, isolation levels, or transaction timeout behavior.
A broker-backed suite is a release gate, not replaced by model tests.

Reject mechanical Java porting: identical internals establish neither semantics
nor performance. See [compatibility matrix](../compatibility.md).

## Reference

- https://kafka.apache.org/39/streams/developer-guide/dsl-api.html
- https://kafka.apache.org/39/streams/core-concepts.html
- https://kafka.apache.org/39/streams/developer-guide/config-streams.html
