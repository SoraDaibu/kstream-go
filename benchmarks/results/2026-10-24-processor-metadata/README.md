# Processor metadata: broker regression baseline

Source `6eddd0c8b8fc2bdfb660a48b5d4c185d9fa9d136` passed
[all verified broker samples and separate profiles](https://github.com/SoraDaibu/kstream-go/actions/runs/40080734191),
all 276 independent reference traces, persistent state and four broker fault
suites. Go 1.27.3, Java 17, Kafka/Kafka Streams 3.9.1, 1M warmup and 1M measured
records, five fresh-process paired trials per workload.

| Workload | Median paired Go/Kafka throughput | Paired range | Go/Kafka median records/s | Go/Kafka median peak RSS KiB |
|---|---:|---:|---:|---:|
| Filter | 1.91x | 1.50–2.53x | 436725 / 233532 | 228584 / 157892 |
| Count | 6.19x | 5.82–6.63x | 587493 / 89596 | 253920 / 228956 |

The runner CPU changed to EPYC 9V74 from the previous EPYC 7763. Both engines'
absolute rates rose; do not attribute that to metadata or compare these as a
controlled before/after. Metadata is not requested by these built-in-only DSL
fixtures. Their normal source handlers remain in use. Filter still misses the
2.5x target and Go peak RSS remains higher in both workloads.

The separate filter profile estimates 2127MB cumulative allocation, with
Task.Emit the largest Go site at about 691MB flat. Input preparation remains
about 181MB. These whole-process samples include startup/warmup/measurement/
shutdown and are not peak memory. Profile timings do not enter comparison ratios.
The next experiment tests explicit output header capacity, with all sizing
tradeoffs retained in local dense/sparse/fanout benchmarks.

Every sample checks output order/count/checksum, extra tails and actual committed
group offsets. One broker, replication factor one, small state, shared CPUs,
different commit/cache policies and no common hard RSS cap prevent production
qualification. Full feature parity is also incomplete.

Both subdirectories retain manifest, environment, samples, summary and the
complete original archive with commands/logs (raw CPU/heap profiles in the
diagnostic archive). Verified SHA256:

- comparison: `02cdcda83664c2e90097712f384c273a3bfc635c81b775ab0c8d6e4f48e6c704`
- profiles: `eb09436c412110b8c04c1e74e014c2bd2305243d799ea787ed24c47be301e619`
