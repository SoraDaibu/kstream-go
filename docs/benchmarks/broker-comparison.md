# Broker-backed comparison protocol

Run `./scripts/compare-brokers.sh results/a-new-run` with Go, Java 17, Maven,
Python 3, taskset and a provisionable Kafka 3.9.1 broker. Set
KSTREAM_KAFKA_BROKERS if it is not localhost:9092. The output directory must be
empty. The workflow runs the same protocol on a disposable single-broker fixture.

The Go application uses RunGroup with a shared persistent Pebble cache; the Java
application uses Kafka Streams 3.9.1 exactly_once_v2 with persistent stores. Both
use one application CPU, three source/output partitions, explicit codecs, no
compression and Kafka transactions for state/output/group offsets. Local stores
are rebuildable caches. Go disables local WAL and synchronous writes; Kafka
Streams uses its ordinary changelog-backed local store configuration. Native
store cache layouts differ and remain part of the recorded experiment.

The independent driver creates fresh source/output/changelog topics, generates
uniform decimal keys and continuous decimal sequence values, warms the app and
measures filter/count workloads. It validates every per-key output in order,
detects duplicate/missing/extra outputs, verifies actual source group offsets and
checks engine checksums. A monotonic atomic send-time ledger maps each output to
its input sequence ID; latency includes input production, processing, transaction
commit and committed-output receipt. It does not misuse event-time timestamps as
latency clocks.

Defaults are 100,000 warmup records, 200,000 measured records, 64 keys and five
fresh-process paired trials per workload. Engine order alternates. Each app uses
the same CPU affinity; the driver uses a different allowed CPU when available.
Go uses GOMAXPROCS=1 and a 512MiB soft heap target; Java uses one active processor
and a 512MiB maximum heap. Native memory has no enforced common cap. Record
observed process peak RSS and CPU snapshots; CPU event boundaries are approximate.
The broker shares the hosted runner and is not CPU-isolated.

The output retains samples.jsonl, summary.json, manifest.json, environment data,
commands and all app/driver/admin logs. Throughput excludes warmup and result
verification. p50/p95/p99/max latency and source-produce duration accompany it;
a source-produce fraction near one may indicate an injection/client bottleneck.
Go commits after each bounded poll (4096 records maximum); Kafka Streams uses a
10ms commit interval and the same maximum poll record count. Different commit
policies and latency outcomes must remain visible.
The Go app also records successful batch counts, size histograms and processing
time separately for warmup and measurement. These are optional runtime
observations, not all Kafka transactions; bootstrap/restore traffic is excluded.
The driver rejects a phase boundary mismatch or an observed input total that
differs from its verified sequence count.

**This is exploratory, not production qualification.** One broker with replication
factor one does not qualify high-availability durability. Shared-host timing,
small state, unmatched native cache layouts, approximate CPU observation and a
missing hard RSS cap also limit inference. Recovery, large/skewed state, joins,
windows, repartitioning and failure workloads remain required. Preserve every
regression and apply the full [qualification rules](../roadmap/qualification.md)
before claiming the 2.5x production target.
