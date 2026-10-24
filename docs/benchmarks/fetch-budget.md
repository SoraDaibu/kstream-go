# Consumer fetch-budget experiment

The retained produce-completion profile still has substantial Kafka client input
decoding allocation and higher Go peak RSS. RunGroup.FetchBytes already exposes
an 8 MiB default fetch target. Smaller targets may reduce eagerly decoded live
records, but may also increase requests or shrink processing/transaction batches.
Do not assume a lower target improves throughput, or that it is a hard memory cap.

Compare 8 MiB, 1 MiB and 256 KiB on the same runner with the same app/driver
binaries and broker. Use three balanced rotating rounds, fresh processes/state/
topics per case, 1M warmup and 1M measured records, filter/count, 3 partitions,
64 keys, poll limit 4096 and explicit output capacity 4096. Rotation puts each
target in each round position. Inputs remain tiny decimal sequence values with
no compression. Retain all manifests, logs, output/order/checksum verification,
actual group-offset checks, batch histograms, latency, observed CPU and peak RSS.

Go-only runs are uninstrumented diagnostics and must not produce Kafka Streams
throughput ratios or a production qualification. Normalize each variant to the
8 MiB sample in the same round and report all ratios plus their median; retain
raw throughput/resource data and negative results. Shared CPUs, unisolated broker,
soft heap targets, one broker and small state remain limitations. Three rounds
are exploratory evidence, not a precise confidence interval.

Expose the fetch target in the benchmark app/manifest and allow an explicit
Go-only harness mode. Keep the library/app/comparative defaults at 8 MiB. Kafka
can return oversized batches despite fetch targets, and compression expands in
memory. Tiny-value results cannot justify a general default change: larger
payloads, multi-input workloads, backpressure and recovery qualification are
still required. Any favorable result needs a separate paired Kafka comparison
before claiming progress toward the project-wide 2.5x target.

Run the [dedicated workflow](../../.github/workflows/fetch-budget.yml), or build
the app/driver once and run against an already provisioned Kafka 3.9.1 broker:

```sh
go build -o /tmp/kstream-fetch-app ./cmd/broker-app
go build -o /tmp/kstream-fetch-driver ./cmd/broker-driver
python3 scripts/fetch-budget-matrix.py \
  --go-app /tmp/kstream-fetch-app --driver /tmp/kstream-fetch-driver \
  --brokers localhost:9092 --output results/fetch-budget
```

The output directory must be empty. Its matrix manifest records binary hashes
and round order, while each leaf retains the complete verified sample. No JVM,
Maven or profiling is required for this Go-only mode. For an explicit standalone
configuration, set KSTREAM_BROKER_FETCH_BYTES and run compare-brokers.sh with
--go-only; ordinary comparisons still require Java and at least five trials.

The [first verified experiment](../../benchmarks/results/2026-10-24-fetch-budget/README.md)
found about 70% lower paired Go RSS at 1 MiB, essentially unchanged filter
throughput and exploratory count improvement with one regressing round. The
paired comparison workflow now explicitly tests 1 MiB on these small-value
fixtures; library/app/script defaults remain 8 MiB. A broader default decision
still requires the missing workload and recovery evidence above.
