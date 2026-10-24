# First paired broker experiment

Source `b200080febd82e1508fcb7d7ee40f80d22b8078d` completed
[the broker comparison workflow](https://github.com/SoraDaibu/kstream-go/actions/runs/40058301396).
All 20 fresh-process samples verified every output, rejected duplicates/missing/
extra records, matched paired engine checksums and checked actual committed input
group offsets. Five alternating trials ran each filter/count engine with 100,000
warmup and 200,000 measured records, 64 uniform keys and three partitions.

| Workload | Median paired Go/Kafka throughput ratio | Go / Kafka median records/s | Go / Kafka median p99 ms | Go / Kafka peak RSS KiB (median) |
|---|---:|---:|---:|---:|
| Filter | 2.06x | 307,678 / 172,918 | 459.4 / 966.5 | 77,844 / 136,368 |
| Count | 6.90x | 516,226 / 75,109 | 286.8 / 2,429.4 | 61,504 / 187,036 |

The paired ratio is the median of each trial's ratio, so it differs from the
ratio of the two displayed throughput medians. Filter ratios ranged 1.12–3.11x;
the exploratory bootstrap interval spans that range. Count ratios ranged
6.67–7.68x. Original values and intervals are in [summary.json](summary.json).

**The 2.5x target is not achieved across these workloads:** filter's median is
below it. This is also not production qualification. The fixture has one broker
and replication factor one on a shared host, small state, different commit
policies/native cache layouts and no enforced common RSS cap. CPU snapshots are
approximate; resource boundaries and all settings are in [manifest.json](manifest.json).
The Go app used about 0.28 CPU seconds during each measured phase; Kafka's medians
were 1.07s for filter and 2.63s for count, observed around driver progress events.
Those values do not certify a controlled CPU or power-efficiency comparison.

The source-produce fraction was about 0.33 for Go filter and 0.38 for Go count.
Throughput includes input injection and committed-output receipt. These burst
latencies and hosted timing cannot replace matched operating-point, stable-host
p99 measurements. Recovery, large/skewed state, joins, windows, repartitioning,
high-availability durability and failure workloads remain qualification gates.

[samples.jsonl](samples.jsonl) retains every sample and resource snapshot;
[environment.txt](environment.txt) records hardware/runtime data. The complete
[original artifact ZIP](original-artifact.zip) includes all app/driver/admin logs,
exact commands and original metadata. Its digest was checked before import:
`sha256:acd7ecf36450911da34a866054e68a5d2d8bb675cd97cd1228601ee25e59a2ff`.
Extract it into a separate directory to inspect the original flat artifact.

See [the executable protocol](../../../docs/benchmarks/broker-comparison.md) and
[qualification rules](../../../docs/roadmap/qualification.md). These measurements
motivate investigation of filter transaction/batch overhead; no unmeasured
optimization or full Kafka Streams parity is claimed.
