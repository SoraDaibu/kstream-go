# ADR 0016: Hopping window counts

Status: Design before implementation; independent oracle and regression gated.

Add window.Hopping{Size, Advance, Grace} and stream.HoppingWindowedCount. Use
Unix-millisecond, half-open windows starting at nonnegative multiples of Advance.
Require 0 < Advance <= Size and nonnegative grace. An input contributes to each
overlapping window in ascending start order, unless that window is closed at
the operator's observed stream time. Closing one window does not drop eligible
overlapping windows for the same input. Null keys skip; null values count.

Share typed count/state/expiry code with tumbling windows, using Advance=Size for
the existing API. Compute the first/last eligible start arithmetically, without
allocating a collection or using interface dispatch per window. A single owned
expiry heap tracks materialized windows; rebuild it from committed state after
recovery and discard it with a failed task. Keep the clock and aggregate
timestamps in transactional stores. Do not add goroutines or channels.

Validate divisible and nondivisible size/advance, boundaries, nulls, late and
out-of-order input against independent Kafka Streams TimeWindows.advanceBy
traces. Test recovery/aborted fanout and closure independently. Measure existing
tumbling benchmarks before/after; preserving allocations is a regression gate.
This does not implement sliding/session windows, suppression or windowed joins.
