# Exploratory grouped-operator comparison

Source `d309947c199e0f0b0a5959eafce23b39083b01d0` passed
[the comparison workflow](https://github.com/SoraDaibu/kstream-go/actions/runs/40051651373).
Five alternating fresh-process trials per workload produced matching output
checksums. Each timed 200,000 records after 100,000 warmup records, with 64 keys,
one active CPU per process, memory stores and no durability.

| Workload | Median paired Go/Kafka throughput ratio | Median Go / Kafka peak RSS (KiB) |
|---|---:|---:|
| Filter | 12.11x | 8616 / 96212 |
| Count | 9.18x | 8624 / 99152 |
| Reduce after value-length mapping | 8.27x | 8636 / 99276 |
| Aggregate value lengths | 7.97x | 8636 / 97476 |

[samples.csv](samples.csv), [manifest.json](manifest.json),
[summary.json](summary.json) and [environment.txt](environment.txt) are original
workflow artifacts. The ZIP digest was verified before import:
`sha256:22500dbc9e33009916ae520971c63935913632e8ba81d3bb24775e30a8f8e271`.
The summary contains exploratory bootstrap intervals from the five paired ratios;
five samples do not establish broad workload or hardware generality.

These are different in-process drivers: Kafka uses TopologyTestDriver and Go uses
its task runtime plus a checksum backend. There are no brokers, persistent stores,
EOS, rebalance, failure injection or tail-latency measurements. They support
further implementation work and **do not qualify the 2.5x production target**.
