# ADR 0004: State durability and transaction coordination

Status: Accepted contract; memory reference implementation only.

## Decision

Each task transaction stages store mutations, output records, and next input
offsets. The backend atomically commits all three. Only after acknowledgement does
the task publish its local state overlay. Commit errors, including ambiguous
outcomes, poison the task: reconstruct from read-committed changelogs and offsets
rather than guessing whether Kafka committed. Replay ignores offsets below the
committed next offset. Offset gaps are legal (Kafka compaction/aborted records).

The backend Open operation returns a fenced generation plus an atomic snapshot
of committed changelogs and offsets. Stale generations cannot commit. Store keys
are scoped by application/task/store. The memory adapter uses a mutex to emulate
the atomic contract; it is not a Kafka protocol implementation and is not durable
across process exit. Restore applies tombstones and never reads uncommitted data.

A future Kafka adapter must provision compacted changelogs with stable names,
use read_committed consumers, transactional production of changelogs and output,
and sendOffsetsToTransaction with current consumer-group metadata. It must fence
old producers, handle rebalances and lost ownership, define transaction timeouts,
and restore before resuming input. Local stores are rebuildable caches; snapshots
must be labelled with committed changelog offsets. Serialization/store errors
abort processing. External RPC or filesystem effects are outside EOS.

## Alternatives

Committing offsets before state loses data. Writing local state before the broker
commit exposes aborted mutations. Independent output and changelog commits are
not EOS. A local WAL alone cannot make output + Kafka offsets atomic. The
prototype accepts extra staging allocations in exchange for a testable protocol;
use a write-batch store and tune transaction batch size later.

## Release gates

Kill at each commit phase; inject timeout after successful broker commit; revoke
ownership mid-batch; restore compacted changelogs; test fencing and offset replay;
compare outputs against an independent Kafka Streams application. Model tests
establish the transaction contract, not production exactly-once guarantees.
