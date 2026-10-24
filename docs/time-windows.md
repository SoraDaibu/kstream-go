# Time-window counts and reduce

WindowedCount accepts window.Tumbling{Size, Grace}. HoppingWindowedCount accepts
window.Hopping{Size, Advance, Grace}. All durations are integral milliseconds;
size and advance must be positive, advance cannot exceed size, and grace must
be nonnegative. Negative timestamps and overflowing window ends are rejected.

```go
counts := stream.HoppingWindowedCount(events, "counts",
    window.Hopping{Size: 10_000, Advance: 5_000, Grace: 2_000})
counts.To("window-counts")
```

Windows start at nonnegative multiples of advance and include [start, start+size).
At timestamp 9 with size 10 and advance 5, the record updates [0,10) followed
by [5,15). At stream-time 15 with grace 5, [0,10) is closed but [5,15) is still
open. A late record at timestamp 9 therefore updates only [5,15). Advances that
do not divide size use the same membership and ordering rules.

Each operator observes the largest accepted timestamp from its own input.
Null keys do not update the clock; present keys with null values count. Every
accepted update emits the maximum timestamp for that key/window lifetime.
Window keys expose Key, Start and End and use the existing JSON codec at sinks.
Choose explicit Kafka-compatible window serialization before external window-key
interoperability; this format is a documented current API boundary.

All fanout updates, expiry deletions, outputs, offsets and the operator clock
stage in one task transaction. Recovery rebuilds the expiry heap from committed
state; failed tasks cannot reuse a partially updated heap. Closed windows are
removed from local state when input advances the clock. There is no additional
post-close query retention, emit-on-close, suppression, sliding/session window
support or windowed join in this API.

## Typed reduce

WindowedReduce accepts a tumbling window; HoppingWindowedReduce accepts a
hopping window. Both use the source value codec and a nullable reducer:

```go
updates := stream.HoppingWindowedReduce(events, "reduced",
    window.Hopping{Size: 10_000, Advance: 5_000, Grace: 2_000},
    func(previous, value string, valueNull bool) (string, bool) {
        if valueNull { value = "<null>" }
        return previous + ":" + value, value == "!delete"
    })
updates.To("window-reductions")
```

For each key/window, the first accepted value initializes state directly without
calling the reducer. Subsequent records call it in processing order. A true null
flag deletes that window's aggregate and emits a tombstone. The next value starts
a fresh lifetime, including its aggregate timestamp; even `!delete` in this example
is ordinary data when it is the first value. Empty strings and numeric zero are
non-null values. Null keys skip without advancing this operator's clock. Null
values advance it. A first null value emits a tombstone without state; a null
value for an existing aggregate invokes the reducer with valueNull=true. This
was independently discovered by the Java reference test and differs from the
unwindowed Reduce null-input policy. Counts still count null values.

Reduce shares counts' membership, ascending hopping order, per-window grace,
operator-local clock and atomic fanout rules. A window that closes does not block
an eligible overlapping window for the same input. Every non-null update stores
the maximum timestamp for its aggregate lifetime; a deletion emits that lifetime's
timestamp too. All updates emit immediately; caching/suppression are absent.

The derived expiry heap is rebuilt from committed live windows. A reduce-only
set prevents heap growth when the same open row is repeatedly deleted/recreated.
It retains one key per distinct unexpired window key, including deleted rows,
until the event-time clock closes the window. It is not a queryable tombstone
store. Failed tasks discard both indexes. Count keeps its specialized integer
state without that set. Each operator loads its committed clock once per task
incarnation and stages every advance; restoration discards any failed cached
clock. Store namespaces belong to their operator and must not be changed by
unrelated custom processors. Existing count state encodings remain unchanged.

Windowed reduce requires a nonempty store, a reducer, valid codecs and already
partitioned keys. Its independent Java scenarios cover tumbling and divisible/
uneven hopping windows; reference validation is required before qualification.
