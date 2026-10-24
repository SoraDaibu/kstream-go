# ADR 0013: Paired broker-backed exactly-once comparison

Status: Measurement design before implementation; no performance claim.

The in-process driver comparison omits brokers, transaction protocols and durable
changelogs. With an experimental group worker available, measure a matched
Kafka Streams 3.9.1 application and the Go implementation against a real broker.
Start with filter and keyed count, equal input/output encodings and full output
verification. Use read-committed output consumers and verify actual committed
source group offsets. Both applications commit state/output/group offsets using
Kafka transactions and use rebuildable persistent local caches.

One external driver creates fresh topics/application identities, warms each
application, sends identical deterministic sequences and verifies every per-key
output. Recover the input sequence ID from filter values or counts/key IDs. A
driver-owned monotonic send-time ledger measures end-to-end committed latency
without relying on event-time timestamps or wall-clock synchronization. Record
throughput, p50/p95/p99 latency, process CPU and peak RSS, versions, topology,
transaction/poll/cache settings and original samples.

Alternate engine order over at least five independent fresh-process trials. Pin
each application to the same allowed CPU and use matching heap targets; keep the
external driver off that CPU when possible. Warmup is excluded from throughput.
Record resource observation boundaries and runtime/native-memory differences.
Use the same source partition count and explicit broker configuration.

This first hosted-CI experiment uses one broker and a shared host. It is an
exploratory broker comparison, not full production qualification. It cannot
certify high-availability durability, stable-host confidence, larger-than-RAM
state, recovery or all required workloads. Report every observed regression and
never substitute in-process speedups or selectively aggregate workloads to
declare the 2.5x target achieved. The qualification rules remain authoritative.
