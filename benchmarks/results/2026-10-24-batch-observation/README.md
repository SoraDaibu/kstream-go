# Committed batch observations

Source `49a723a68d2a56bc6e7d9b3128e17bfa3f488356` passed
[the instrumented broker experiment](https://github.com/SoraDaibu/kstream-go/actions/runs/40060243968).
All 20 samples verified outputs/group offsets and matched observed record totals
to the independent driver. Phases remained separate: 100k warmup and 200k measured
records, with 4096 as the maximum poll size.

| Workload | Median processing batches | Median mean records/batch | Median paired Go/Kafka throughput ratio |
|---|---:|---:|---:|
| Filter | 53 | 3773.6 | 2.79x |
| Count | 51 | 3921.6 | 8.26x |

Filter trials had 47–49 batches in the 4096 size bucket and only a few small
batches. Most polling was already full; this does not support adding a linger
delay to fix the first run's shortfall. Per-phase histograms and cumulative
processing times are retained in [samples.jsonl](samples.jsonl).

Filter paired ratios still varied from 1.48–3.16x, and early Go filter phases
took longer than later ones. Warmup and system effects need further investigation.
Neither the median above 2.5x nor differences from the first run qualify a stable
production speedup. Hosted CPU, broker contention, small state, native caches
and commit policies remain limits. The runtime polling policy is unchanged.

[summary.json](summary.json), [manifest.json](manifest.json) and
[environment.txt](environment.txt) retain measurements and context. The complete
[original artifact](original-artifact.zip) contains all logs/commands and has
verified SHA256 `821d551ead24ca249cb00ad1ccd72dc200d984ad033ace4ef2573f53c61b06d9`.
The next CI experiment increases warmup and measurement length; it must be
reported separately rather than replacing these observations.
