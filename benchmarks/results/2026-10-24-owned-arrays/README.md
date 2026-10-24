# Owned transaction record arrays: broker check

Source `220adb5d7f8cd8c95bbaa4467aaabbff3d2f54c8` passed
[all twenty verified broker samples and separate profiles](https://github.com/SoraDaibu/kstream-go/actions/runs/40076754899),
ordinary/differential CI, persistent state and all four fault suites. Both this
and the preceding baseline use Go 1.27.3 and hosted EPYC 7763 CPUs; they are
separate shared runner jobs, not a controlled paired before/after experiment.

| Workload | Median paired Go/Kafka throughput | Range | Go/Kafka median records/s | Go/Kafka median peak RSS KiB |
|---|---:|---:|---:|---:|
| Filter | 2.09x | 1.40–2.46x | 396432 / 191217 | 263424 / 153816 |
| Count | 6.38x | 5.40–6.65x | 401782 / 61541 | 267756 / 230808 |

Filtering remains below 2.5x. Small median changes from the earlier 2.04x/6.09x
ratios do not establish an application-level speedup. RSS remains higher for Go
in both workloads. The allocation change is independently measured in the
[local assembly report](../../../docs/benchmarks/2026-10-24-group-assembly/README.md).

The separate sampled filter allocation profile attributes 333.87MB flat to
CommitTasks versus 383.21MB in the preceding Go 1.27 baseline, about 12.9% less,
consistent with the isolated byte reduction. Whole-process sampled allocation
is nearly unchanged (2664MB versus 2659MB): other sites and sample variation
remain. RunGroup input-slice growth and Task.Emit output-slice growth are still
the largest remaining sites. This does not establish lower total memory from
record arrays, and retained-pointer lifetime remains a documented tradeoff.

Each uninstrumented case has 1M warmup and 1M measured records, fresh topics,
verified outputs/checksums/group offsets, five alternating paired trials and
small 64-key state. Profile timings do not enter comparison ratios. One broker,
replication factor one, shared CPUs, different commit/cache policies and no
common hard RSS cap prevent production qualification. Full feature parity is
also unfinished.

Both subdirectories retain samples, summary, manifest, environment and the
complete original archive including commands/logs; profiles also contain raw
CPU/heap profiles and text reports. Verified archive SHA256:

- comparison: `d9fdbd58f3ace82c6672326e6fbb5cc693e8fed64f0638c47ce92bcb595d8a8b`
- profiles: `bca05a1fe6b1880c434236c55dcecebd77deeeaea8a503f7b88f1873016df107`
