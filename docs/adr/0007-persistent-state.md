# ADR 0007: Task-owned stores on a shared persistent engine

Status: Accepted for the experimental adapter; production restoration/performance gated.

## Context and decision

The execution layer currently embeds a map image. Persistent local stores need
an explicit storage contract, I/O failure semantics and lifecycle ownership;
placing a disk database directly in DSL operators would break layer boundaries.

Define a state.Store contract for owned reads, store scans, atomic mutation
batches, reset and close. Memory remains the default implementation. An optional
execution store factory creates one exclusively owned logical store per task.
The DSL and logical topology do not import storage engines. Read/scan errors are
explicit in Processor Context and independently recorded by the task so a custom
callback cannot accidentally commit after ignoring an I/O error. This refines an
experimental API before a stable release; null and missing values remain distinct.

The persistent adapter uses Pebble's public Go API. One physical engine serves
multiple disjoint task namespaces and a bounded set of engine background workers;
there is no database instance per operator. A namespace has one logical owner,
acquired and released during task setup/close. Engine synchronization protects
physical infrastructure; a task continues to own its mutable processing state.
Shared physical storage does not permit sharing a task's logical namespace.

## Commit and failure order

Processing stages state mutations and outputs in the task overlay. Commit the
Kafka/backend transaction first, then apply the local-store write batch. If a
local apply fails after Kafka committed, poison the task and reject reads/reuse;
restore a fresh task from committed changelogs. Never retry the input in-place
or pretend the two independent commit decisions are one distributed transaction.
Read and scan failures abort before the backend commit even if a callback ignores
the returned error. Store setup failure occurs before fencing a previous backend
owner. Task close fails while processing is active and releases its store once.

A new task resets its exclusively owned cache and loads the complete committed
snapshot. This is conservative and cannot accidentally expose an uncommitted or
stale local cache after a crash. Checkpoint-based reuse requires a proven matching
changelog offset and schema version; it is not inferred from input offsets alone.

## Durability and limits

Pebble supports explicit synchronous writes for independently durable local data.
When used only as a Kafka-backed rebuildable cache, non-synchronous writes may be
selected explicitly; no local power-loss guarantee is claimed in that mode. Any
performance comparison must match these durability settings to the reference.

The first implementation still restores through the existing in-memory backend
snapshot. Streaming restore into the store and validated checkpoint reuse are
required before claiming bounded recovery memory for state larger than RAM.
Versioned/window/session store APIs, efficient retention/range queries, standby
replication and interactive-query routing remain separate parity work.

## Validation

Test owned byte buffers, store and task namespace isolation, batch atomicity,
reset, close/lease ownership and reopen persistence. Inject read/scan failures,
backend failures and local-write failure after successful backend commit. Verify
that recovery uses committed positions without duplicating output. Run the same
semantic oracle and broker tests with the store option; record memory and
throughput changes before any production qualification claim.
