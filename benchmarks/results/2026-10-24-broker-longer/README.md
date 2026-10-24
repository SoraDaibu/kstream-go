# Longer warmup and measurement

Source `a360d99056918c23d1adb0239334af2549f1480d` passed
[all twenty broker samples](https://github.com/SoraDaibu/kstream-go/actions/runs/40061001840).
Each case uses one million warmup and one million measured records, five paired
trials per workload, with verified per-key output/checksums and committed source
group offsets. No polling/runtime tuning was applied.

| Workload | Median paired Go/Kafka throughput | Paired range | Go/Kafka median records/s | Go/Kafka median peak RSS KiB |
|---|---:|---:|---:|---:|
| Filter | 1.83x | 1.09–2.16x | 349714 / 185693 | 254868 / 152516 |
| Count | 6.42x | 6.27–6.70x | 386691 / 59148 | 256552 / 230584 |

Filter remains below the 2.5x target, and Go peak RSS is higher for both workloads
in this run. Median p99 latency is 1700/4147ms for Go/Kafka filter and
1690/15486ms for count. Most batches were already full: both workloads have
248 median processing batches, averaging 4032 of the 4096-record maximum.
Longer warmup alone does not establish the target. Allocation/CPU profiling is
the next diagnostic step; these results do not justify adding input linger.

The hosted CPU here is EPYC 7763; the preceding short instrumented run used
EPYC 9V74. Different hosts, trial sizes, SMT contention, broker scheduling,
commit policies and cache layouts prevent attributing their difference to one
cause. Absolute rates, latency and memory remain workload and host dependent.
This remains a single broker with replication factor one and small 64-key
state, with no HA/recovery throughput or common hard native-memory cap.
**Production qualification is false; full feature parity is also unfinished.**

The script's historical defaults remain 100k/200k; reproduce this run with:

```sh
KSTREAM_BROKER_WARMUP=1000000 KSTREAM_BROKER_RECORDS=1000000 KSTREAM_BROKER_TRIALS=5 ./scripts/compare-brokers.sh results/new-longer-run
```

[summary.json](summary.json), [samples.jsonl](samples.jsonl),
[manifest.json](manifest.json) and [environment.txt](environment.txt) retain the
observations. The [original artifact](original-artifact.zip) contains every
command and app/driver/admin log. Its verified SHA256 is
`052680891485566c27ceb69daf20f4927c3267570a1991322ff67670a1f51368`.
