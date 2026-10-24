# Independent semantic oracle

Run `./scripts/differential.sh` with Go >= 1.27, Python 3, Java >= 17 and Maven.
Maven downloads Kafka Streams **3.9.1**, test-utils, RocksDB and build plugins.
There is no Kafka broker dependency: Java uses the public TopologyTestDriver.
The oracle is original test code, not a port of Kafka internals.

Both programs receive identical ordered topic/timestamp/key/value TSV traces and
compare exact output topic/key/value/timestamp rows. Go runs transaction batch
sizes 1, 32 and 256 against the same Java output; batching must preserve every
record update and event-time result. Each batch size runs both memory and Pebble
task stores, using both independent and grouped task commits, with output capacity
hints 0 and 256. This configures 960
exact trace comparisons across forty scenarios. `~` represents null; empty
fields represent non-null empty strings. The fixed boundary cases are followed
by 300 generated rows with seed 73019 per scenario. Caching is disabled to make
update traces observable. Scenarios cover ordered first-match branching with and without catch-all routes, filter/filterNot, null-aware mapValues, flatMap, flatMapValues,
merge, explicit broker-exchange semantics, connected-stage clock isolation,
source topic/partition/offset through processors and fanout,
cross-type processors with numeric intermediate keys/values, UTF-8 byte lengths,
skipped records, nulls and changed timestamps,
grouped count/reduce/aggregate (including nullable initialization), table upserts
and tombstones, table filter/filterNot/nullable value maps and chained lookups,
stream-table inner/left joins with nullable results, and tumbling/hopping windows with size 10ms and
grace 5ms. Typed windowed reduce covers tombstones, first-value initialization,
reinitialization timestamps, null-key clock isolation and null-value clock
advancement with the same bounds. The null-input fixture preserves the first
windowed-reduce reference mismatch. Typed window aggregates cover both ordinary
and nullable initialization, nullable inputs/results and typed numeric state.
Hopping advances of 5ms and 6ms cover divisible and uneven overlap.
Streams that change keys are not grouped without a broker exchange.

A mismatch is a correctness failure. Minimize it, commit a regression fixture,
and update the compatibility matrix only after an explicit semantic decision.
Never change expected output just to pass the suite. This suite does not cover
rebalance, Kafka transactions, producer fencing, network failures, repartition,
compaction, or restored RocksDB. Separate broker tests cover fixed-assignment
transactions, group restart/rebalance, process kills and Kafka/Pebble recovery.
Forced compaction and broader network/coordinator failures remain open gates.

Operator-local versus unrelated-source stream-time is also tested in Go. No
results are claimed until the Java and Go programs have actually run successfully.

Explicit repartition traces compare committed exchange delivery and changed-key
grouping with the public Java repartition API. The Go model drains only committed
exchange outputs into a separate processing transaction; real Kafka tests cover
physical routing, restart, aborted exchange visibility and actual group offsets.


Clock cases require an explicit harness boundary. Kafka Streams 3.9.1's
TopologyTestDriver creates one simulated StreamTask for the whole topology,
including disconnected and repartitioned sections. A single-driver attempt
therefore produced 10000 for an unrelated-stage clock; the minimized input is
retained in [this fixture](fixtures/clock-driver-task-isolation.json).
[ClockOracle](kafka-streams/src/main/java/dev/kstream/ClockOracle.java) first
asserts that the original public DSL describes two subtopologies, then pins the
single-driver behavior. It evaluates each task component with its own driver,
routing serialized exchange outputs explicitly through a new public input topic.
All processors, null-key filtering and currentStreamTimeMs come from Kafka's
public APIs; no Go component compiler or Kafka internal code is reused.

These two composed-driver cases test per-task processing clocks and explicit
exchange records. They do not test Kafka's production task assignment, consumer
scheduling or transaction transport. The real Go broker suite separately checks
exchange recovery with late input. A live Java/Go clock comparison remains a
broader qualification item. The harness limitation is visible in the pinned
[3.9.1 test-driver construction](https://github.com/apache/kafka/blob/3.9.1/streams/test-utils/src/main/java/org/apache/kafka/streams/TopologyTestDriver.java#L519),
which constructs one StreamTask with all input partitions. Reading that code
diagnosed the test harness; no implementation is ported from it.
