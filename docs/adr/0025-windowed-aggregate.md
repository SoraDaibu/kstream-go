# ADR 0025: Typed window aggregate state

Status: accepted for implementation; independent reference validation required.

Add WindowedAggregate and HoppingWindowedAggregate with explicit result codecs,
nullable initializers, nullable input values and nullable aggregate results. Use
the existing tumbling/hopping boundaries and task-owned lifecycle. Kafka Streams'
window operators accept null values, as independently found for count/reduce;
do not copy the unwindowed aggregate skip-null policy without reference evidence.

Go callbacks receive (key, input, inputNull, previous, previousNull), avoiding
interface{} or pointer-allocated option wrappers. Initializer runs for an absent
aggregate before the aggregator, including a null-valued first input. A null
result deletes state and emits a tombstone; subsequent input initializes again.
Preserve maximum timestamps within each aggregate lifetime and ascending open
window order. These details remain subject to the independent public Java oracle.

Factor reduce and aggregate into one private typed window fold with a supplied
apply function and result codec. Reduce still bypasses its reducer for a first
value, including a first null tombstone. Keep counts' specialized integer hot
path, stable state format and separately benchmarked null semantics. All three
reuse the shared operator clock/restore/expiry logic. Reduce/aggregate share the
tracked-key set that prevents duplicate expiry entries on deletion/recreation.

No asynchronous operators, goroutines, per-record interface conversion, runtime
reflection or reusable published buffers are needed. Generic aggregate state
uses the existing codec/envelope and participates in the task transaction. Task
poison/recovery invalidates derived clocks/indexes after any failed transaction.

Validate nullable initialization/input/output, first update/reinitialization,
empty and zero aggregates, typed state recovery, per-window grace, partial
overlap closure and failure during fanout. Add independent Java aggregate traces
for tumbling and divisible/uneven hopping, with both nullable and ordinary
initializers. Measure reduce before/after factoring its loop. Full specialized
window stores, queries, caching, suppression, emit strategies, sliding/session
windows and broker retention qualification remain separate work.
