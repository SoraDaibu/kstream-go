# Independent semantic oracle

Run `./scripts/differential.sh` with Go >= 1.23, Python 3, Java >= 17 and Maven.
Maven downloads Kafka Streams **3.9.1**, test-utils, RocksDB and build plugins.
There is no Kafka broker dependency: Java uses the public TopologyTestDriver.
The oracle is original test code, not a port of Kafka internals.

Both programs receive identical ordered topic/timestamp/key/value TSV traces and
compare exact output topic/key/value/timestamp rows. Go runs transaction batch
sizes 1, 32 and 256 against the same Java output; batching must preserve every
record update and event-time result. `~` represents null; empty
fields represent non-null empty strings. The fixed boundary cases are followed
by 300 generated rows with seed 73019 per scenario. Caching is disabled to make
update traces observable. Scenarios cover filter, grouped count, table upserts
and tombstones, stream-table inner join, and tumbling windows with size 10ms and
grace 5ms. Streams that change keys are not grouped without a broker exchange.

A mismatch is a correctness failure. Minimize it, commit a regression fixture,
and update the compatibility matrix only after an explicit semantic decision.
Never change expected output just to pass the suite. This suite does not cover
rebalance, Kafka transactions, producer fencing, network failures, repartition,
compaction, or restored RocksDB. Those require a future broker-backed suite.

Operator-local versus unrelated-source stream-time is also tested in Go. No
results are claimed until the Java and Go programs have actually run successfully.
