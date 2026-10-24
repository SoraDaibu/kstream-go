# Explicit output capacity: broker validation

Source `58a50254fb02f8bc5eaa59aacd90becce750fe42` passed
[all verified broker samples and separate profiles](https://github.com/SoraDaibu/kstream-go/actions/runs/40083138035),
all 552 reference comparisons, persistent-state integration and all four broker
fault suites. Go 1.27.3, Java 17, Kafka/Kafka Streams 3.9.1, 1M warmup and 1M
measured records, five fresh-process alternating engine pairs per workload.

The explicit Go output-capacity hint is 4096 for these known one-output-per-input
fixtures. The library, app and script defaults remain zero. The manifest records
this setting. Local sizing evidence includes sparse/fanout regressions; a hint
should follow an application's emission rate, not an assumed generic default.

| Workload | Median paired Go/Kafka throughput | Paired range | Go/Kafka median records/s | Go/Kafka median peak RSS KiB |
|---|---:|---:|---:|---:|
| Filter | 2.03x | 1.59–2.56x | 417518 / 193739 | 232860 / 155400 |
| Count | 7.20x | 5.71–7.59x | 450386 / 62386 | 250560 / 230400 |

Filter still misses the 2.5x target. Go peak RSS remains higher. The host is EPYC
7763, versus EPYC 9V74 in the preceding metadata baseline; these are not
controlled before/after throughput measurements. Both engines' absolute rates
fell. Use the predeclared median of paired ratios, not selected trials.

The separate filter profile estimates 1629MB whole-process cumulative allocation,
with Task.Emit flat 186MB versus 691MB in the prior profile (about 73% lower).
Whole-process allocation is about 23% lower. Input preparation is 168MB and
GroupBackend commits 324MB. This supports the targeted allocation reduction;
the sampled totals include startup/warmup/measurement/shutdown and are neither
peak memory nor comparative throughput samples. GC and Kafka client record
allocation remain material costs.

Every sample independently checks output order/count/checksum, extra tails and
actual committed group offsets. One broker, replication factor one, small state,
shared CPUs, different commit/cache policies and no common hard RSS cap prevent
production qualification. Full feature parity also remains incomplete.

Both subdirectories retain manifest, environment, samples, summary and the
complete original archive (including commands/logs and diagnostic raw profiles).
SHA256 verified against the Actions artifact metadata:

- comparison: `37492f47dd6ff935362dd6b6fcd9897869430e33775e1f64a4fbd412ee7a41b6`
- profiles: `d2d130f5062dc3d5ad35af5863d3672f76f55e6c27b144fb91f44fa8d435a42b`
