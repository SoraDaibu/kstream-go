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
hints 0 and 256. This configures 648
exact trace comparisons across twenty-seven scenarios. `~` represents null; empty
fields represent non-null empty strings. The fixed boundary cases are followed
by 300 generated rows with seed 73019 per scenario. Caching is disabled to make
update traces observable. Scenarios cover ordered first-match branching with and without catch-all routes, filter/filterNot, null-aware mapValues, flatMap, flatMapValues,
merge, source topic/partition/offset through processors and fanout,
cross-type processors with numeric intermediate keys/values, UTF-8 byte lengths,
skipped records, nulls and changed timestamps,
grouped count/reduce/aggregate (including nullable initialization), table upserts
and tombstones, table filter/filterNot/nullable value maps and chained lookups,
stream-table inner/left joins with nullable results, and tumbling/hopping windows with size 10ms and
grace 5ms. Hopping advances of 5ms and 6ms cover divisible and uneven overlap.
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
