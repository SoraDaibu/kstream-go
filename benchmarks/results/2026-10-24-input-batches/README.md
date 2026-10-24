# Exact input batches: verified broker result

Source `aa0cff0392af8b0444f0e9488df47854ca737962` passed
[all verified broker samples and separate profiles](https://github.com/SoraDaibu/kstream-go/actions/runs/40077538258),
all 252 differential comparisons, persistent stores and four broker fault suites.
Go 1.27.3, Java 17, Kafka/Kafka Streams 3.9.1, hosted EPYC 7763, 1M warmup and
1M measured records, five fresh-process paired trials per workload.

| Workload | Median paired Go/Kafka throughput | Paired range | Go/Kafka median records/s | Go/Kafka median peak RSS KiB |
|---|---:|---:|---:|---:|
| Filter | 1.90x | 1.46–2.60x | 406504 / 194402 | 253932 / 151936 |
| Count | 6.50x | 5.22–7.41x | 437722 / 64492 | 251768 / 230608 |

The paired filter median is lower than the preceding run's 2.09x even though
observed Go absolute throughput/CPU/RSS are better. Do not call that a stable
speedup or suppress the shortfall. Median paired ratios are computed per trial;
they need not equal the ratio of two separate median rates. Host load, JVM and
broker variability remain uncontrolled. Filtering still misses the 2.5x target,
and Go peak RSS remains higher in both workloads.

The independent profile shows a clearer allocation effect. Input preparation
uses about 168.46MB sampled flat allocation versus 691.58MB in the preceding
RunGroup input-slice path, about 75.6% less. Total sampled filter allocation is
2101.77MB versus 2664.44MB, about 21.1% less. Task.Emit remains the largest Go
allocation site at 656.90MB. These are whole-process estimates including warmup
and startup/shutdown, not peak memory. Instrumented timings never enter the
paired-comparison ratios. The [isolated routing data](../../../docs/benchmarks/2026-10-24-input-batches/README.md)
retains same-binary old/new samples and its limits.

Every uninstrumented sample verifies output order, counts, checksums, extra-tail
absence and actual committed group offsets. Batching remains near full, with
249 median processing batches averaging 4016 records. One broker with replication
factor one, small state, shared CPUs, differing commit/cache policies and no
common hard RSS cap prevent production qualification. Full feature parity is
unfinished.

Both subdirectories retain manifest, environment, samples, summary and the
complete original archive, including every command/log and raw CPU/heap profiles
in the diagnostic archive. Verified SHA256:

- comparison: `16825763ce7e97fe2f38f5d92f7f1989c949df316cd4586ca5b8f580bc193b15`
- profiles: `c2a0aaea621d960c9352252ece414bf45ec24fbcf1f11e0a2e72f7179e00142a`
