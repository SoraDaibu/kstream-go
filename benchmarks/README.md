# Comparison program

`./scripts/compare-engines.sh` runs paired, alternating fresh-process trials of
this Go core and Kafka Streams 3.9.1 TopologyTestDriver. It is an **exploratory
in-process comparison**, not the production qualification target.

Both scenarios use string inputs, 64 keys, the value `value`, explicit timestamps,
per-record processing, serialized output decoded before checksumming, and no
state durability. The aggregation uses Kafka's in-memory key-value store with
logging and caching disabled; Go uses task-local memory and an ephemeral checksum
backend. Each engine warms the same state before measuring. Five independent
trials alternate order. Checksums must match exactly before a ratio is reported.

The runner fixes GOMAXPROCS/ActiveProcessorCount to one, limits Go heap target and
Java maximum heap to 512 MiB, and records Linux peak RSS. These runtime memory
settings are not identical hard process limits. Go and Java driver overheads
also differ. Startup is outside the timed region. Short trial/JIT/host noise,
missing brokers, missing durable state and missing latency measurements prevent
this harness from qualifying the 2.5x production target, regardless of its ratio.

Outputs retain the manifest, environment, per-trial CSV, checksums, RSS and paired
median ratios with exploratory bootstrap intervals. A failed checksum or missing
trial fails the run. See [qualification rules](../docs/roadmap/qualification.md)
for the full end-to-end broker, EOS, recovery, latency and resource gates.
