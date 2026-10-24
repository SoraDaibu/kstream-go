# ADR 0008: Typed grouped aggregation and nullable results

Status: Accepted for the tested non-windowed, existing-key subset.

Expose KGroupedStream through KStream.GroupByKey. It describes a grouping by
the existing key and does not perform a broker exchange. The existing Count
function remains available; grouped Count delegates to its specialized int64
implementation. Key-changing streams must still repartition before grouping.

Type-preserving Reduce is a grouped-stream method. Type-changing Aggregate is
a package function because Go methods cannot introduce additional type
parameters. Callers supply the aggregate serde and an initializer. Nullable
initializer/results use the existing API convention: a bool means null; a Go
zero value remains valid data. The aggregator also receives the old aggregate's
null flag, matching Kafka's nullable initializer behavior without reflection,
interface boxing or sentinel zero values.

Aggregate state stores serialized value plus its maximum accepted timestamp.
Null input keys or values are skipped. A null aggregate result deletes the
stored value and emits a tombstone. A later event initializes a new aggregate;
its timestamp begins a new lifetime. Reduce uses the first input value directly
instead of calling the reducer with a fabricated zero value.

Keep operators typed and synchronous and keep state inside the task store. No
goroutines, channels or shared aggregate map are introduced. Processor factories
bind codecs and callbacks during task compilation. Callbacks must return owned
or immutable values; serializers must not encode non-null values as nil bytes.

Validate ordinary and nullable initialization, deletions/reinitialization,
out-of-order timestamps, null inputs, codec failures, restart/replay and downstream
table lookup. Compare exact output traces with Kafka Streams 3.9.1, using memory
and persistent stores at batch sizes 1, 32 and 256 before claiming equivalence
for these operators. Consumer-group, exchange and window aggregation parity
remain separate gates.
