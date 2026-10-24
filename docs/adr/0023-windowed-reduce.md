# ADR 0023: Time-windowed reduce and shared window lifecycle

Status: accepted; all 720 independent reference comparisons passed at source
`908680e75a4f99703a53a04242122980f4e94ce4`.

Extend tumbling and hopping windows from counts to typed reduce. Add
WindowedReduce and HoppingWindowedReduce, returning typed window-key update
streams like the existing count API. Preserve first-value initialization, input
order within each window, maximum aggregate timestamp, nullable reducer results,
and per-window grace. A null result deletes the aggregate and emits a tombstone;
the next accepted value initializes a fresh aggregate lifetime. Skip null input
keys. A null value advances the clock and, for an absent aggregate, emits a
tombstone without invoking the reducer. Existing aggregates pass valueNull=true
to the reducer. The first independent Kafka Streams 3.9.1 trace rejected the
initial skip-null-value assumption; preserve that finding in a regression fixture.

Share task-owned operator-clock, restore validation and expiry-heap bookkeeping
between counts and reduce. Keep count's specialized integer state and arithmetic
window enumeration. Reduce uses the existing typed aggregate envelope and its
explicit value codec. No reflection or interface-boxed aggregate state is needed.
State mutations, clock changes, outputs and offsets use the same task transaction.
Failure discards the in-memory derived index along with the task incarnation.
Load the operator-owned durable clock once per incarnation, then cache its value
and stage every advance. This avoids copying/decoding the same stored clock on
every input. The cached clock must also be discarded after any failed transaction;
operator store namespaces must not be mutated by unrelated custom processors.

For reduce, deletion/recreation of an unexpired window must not accumulate a
heap entry on every update. Maintain a reduce-only set of tracked window keys;
remove entries on expiry, and rebuild only committed live windows on recovery.
An already tracked deleted key can be recreated without duplicating its expiry.
Counts never delete an open aggregate, so they retain the existing heap without
the extra set. Retaining a deleted reduce key until its window closes is a
documented bookkeeping cost bounded by distinct unexpired window keys.

Reject invalid window bounds, empty stores, nil reducers, invalid codecs and
key-changing inputs without a real repartition boundary. Keep ascending hopping
window order and overflow validation. Empty values remain ordinary data. Existing
count null-value semantics must not change while reduce exposes nullable input to its callback.

Validate late/grace boundaries, overlapping partially closed windows, null/empty
records, delete/reinitialize timestamps, recovery, failure during hopping fanout
and independent operator clocks. Differential tests use public windowed reduce
with tumbling/divisible/uneven windows. Measure the existing count benchmark
before/after with Go 1.27.3; allocation changes require investigation. These
updates do not add window aggregates, suppression, sliding/session windows,
interactive window queries or a production retention qualification.
