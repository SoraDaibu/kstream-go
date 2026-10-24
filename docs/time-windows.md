# Time-window counts

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
