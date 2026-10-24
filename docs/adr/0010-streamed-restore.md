# ADR 0010: Bounded-batch restoration into task stores

Status: Accepted for the experimental Kafka and memory backends after failure and broker validation.

The original Backend.Open returns a complete in-memory state image. Copying that
image into Pebble preserves correctness but defeats the purpose of persistent
stores for state larger than RAM. Retain Open for compatibility and small model
backends, and add an optional execution.StreamingRestorer capability.

OpenInto fences previous ownership and synchronously delivers bounded ordered
mutation batches to a restore sink, returning only committed input offsets and
stream-time. Runtime capability detection occurs once during task setup. Reset
the exclusively owned local store before opening the backend, then apply restore
batches directly. Publish the task only after the full committed prefix restores.
On any error, close the unpublished store and require a fresh incarnation.

Restore batches may cross original transaction boundaries: Kafka read-committed
isolation exposes only committed changes, and no task is accessible during
initialization. The same state key may occur repeatedly; applying log order
preserves final state and tombstones. A sink must copy borrowed mutation/value
buffers before returning if it retains them. A callback error aborts restoration
and must not publish offsets, a healthy backend or a partially restored task.

The Kafka implementation keeps the existing unique committed restore barrier and
producer fencing protocol. Flush bounded state batches before returning at the
barrier. Read metadata independently and retain the latest complete checkpoint.
Cap restore fetch targets and record batches; Kafka may deliver one oversized
record batch despite a fetch target, so broker message/batch limits also bound
memory. Legacy Open collects the same stream into an image for callers that
explicitly need one.

The reference memory backend can stream its committed model log. It still retains
the entire log by design; this capability does not turn it into a durable engine.
The default memory task store also remains proportional to state. Pebble-backed
key-value recovery removes the extra complete image; window processors still
build an O(N) expiry heap and local checkpoint reuse remains unimplemented.

Test capability selection without invoking legacy Open, batch ordering and
tombstones, partial restore failure/cancellation, cleanup of unpublished stores,
legacy fallback, replay and metadata. Broker tests must force multiple restore
batches and compare legacy versus streamed committed state, including aborted
records. Measure restore allocation scaling independently of broker throughput
before making a bounded-memory production claim.
