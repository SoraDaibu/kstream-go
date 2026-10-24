# Current broker comparison evidence

These two runs retain the unchanged filter/count workloads at the repartition
and connected-clock source revisions. They do not benchmark repartition traffic
or scoped processor clocks. Both use Go 1.27.3, Kafka/Kafka Streams 3.9.1,
1M warmup and 1M measured inputs, five fresh-process alternating engine pairs,
three partitions, 64 uniform keys, output capacity 4096 and an explicit 1 MiB Go
fetch target. Library defaults remain 8 MiB. See each manifest and complete ZIP
for commands, exact verification, samples, resource observations and profiles.

| Source/run | Filter median Go/Kafka throughput | Count median Go/Kafka throughput |
|---|---:|---:|
| [Repartition source](https://github.com/SoraDaibu/kstream-go/actions/runs/40102903514), 5ab0e951 | 2.089× | 6.887× |
| [Connected-clock source](https://github.com/SoraDaibu/kstream-go/actions/runs/40104725468), 827c9cf0 | 2.205× | 7.002× |

Both filter medians miss the predeclared 2.5× target. All five connected-clock
filter ratios are below 2.5 (the largest is 2.495, not a passing result rounded
to 2.50). The earlier 1 MiB run's 2.583× filter median does not establish a stable
performance advantage. Retain the below-target runs alongside that earlier run;
do not choose a favorable machine or trial, or average count's margin over filter.

| Workload/revision | All paired throughput ratios |
|---|---|
| Filter / repartition | 1.294, 2.089, 1.715, 2.692, 2.189 |
| Filter / connected clock | 1.521, 2.495, 2.205, 1.912, 2.207 |
| Count / repartition | 5.441, 7.690, 6.982, 6.749, 6.887 |
| Count / connected clock | 7.040, 7.055, 6.801, 6.932, 7.002 |

The connected-clock run's Go/Kafka median rates are 430,401/194,996 records/s
for filter and 442,678/63,647 for count. Approximate observed CPU is
1690/3750 ms and 1730/11050 ms; peak RSS is 66044/155508 KiB and
69124/227428 KiB. Median p99 driver receipt latency is 1189/3852 ms and
1245/14282 ms. Ratios are medians of paired rates, not ratios of separate
absolute medians. Go averages about 3861 inputs per transaction in both runs.

Absolute Go and Kafka rates both fall in the later run, despite a larger paired
ratio. These are separate shared hosted runners; no change here establishes a
causal speedup or slowdown. Tiny values/state, one broker/RF1, different commit
policies, soft heap targets rather than a matched hard RSS cap, producer limits,
and no joins/windows/recovery throughput remain material limitations. Broader
controlled-host performance and feature parity are still **not qualified**.

All cases verify exact committed output/order/checksums, extra tails and actual
group offsets. Profiles are separate diagnostic Go-only runs and are not mixed
into the uninstrumented comparison timings. The complete original archives are
retained so Actions artifact expiration does not discard evidence.

[archives.json](archives.json) records source revisions, workflow runs and
SHA256 digests verified against Actions metadata. Selected JSON/environment files
are also extracted for review; all logs, commands and raw profiles remain in the
original ZIPs.

The [Go 1.27.3 scheduling follow-up](../../../docs/benchmarks/2026-10-24-scheduling-go127/README.md)
is a separate finite-task/no-op-backend experiment. It does not qualify Kafka
throughput, durability, live fairness or tail latency.
