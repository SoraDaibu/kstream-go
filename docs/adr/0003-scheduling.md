# ADR 0003: Task scheduling and concurrency

Status: Provisional; benchmark-driven production choice is open.

## Decision

A task owns all stores and operator closures for its partition. Process is serial
and rejects concurrent entry; no operator goroutines/channels. A bounded worker
scheduler runs explicit batches from independent task lanes. Every task appears
at most once in a submitted run, preserving order and preventing split ownership.
The initial batch scheduler does not provide fairness for an unbounded stream;
a production scheduler needs bounded record quanta and bounded fetch queues.

Keep both bounded-worker and goroutine-per-task batch strategies in the runtime
for measurement. Worker count bounds concurrent task execution. Per-task mode
exists for comparison and is opt-in. Neither implementation creates a goroutine
per record or per operator. Cancellation is checked before each record. Errors
cancel further scheduling; already committed independent task records stay
committed. Task processing errors poison that incarnation so callback-local state
cannot escape an aborted transaction into a retry; restore a new task instead.

## Benchmark gate

Run BenchmarkSchedule across task counts, worker counts, records per batch, and
CPU work per record with -benchmem. Include GOMAXPROCS=1 and a multi-core run.
Measure throughput, allocations, goroutines and p95/p99 latency for future live
scheduling; the current benchmark measures finite-batch throughput only. Separate
scheduler overhead from transaction/serialization costs. Store environment,
commands and raw output. Never infer a production winner from a single microbench.

## Alternatives and consequences

Goroutine-per-task is idiomatic and can be attractive for a bounded assignment,
but large assignments and blocking callbacks change memory/latency. Worker
scheduling bounds active execution but introduces queueing and potential hot-task
starvation. A global store mutex obscures ownership and is rejected. No final
scheduling claim is made before representative broker-backed measurements.
