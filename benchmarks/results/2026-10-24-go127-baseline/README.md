# Go 1.27.3 broker baseline and independent profiles

Source `c0d66893fd12e76db1d61bfdc690dc3e7bbde90e` passed
[verified broker workloads and separate profiles](https://github.com/SoraDaibu/kstream-go/actions/runs/40075877893).
Actions checked out merge `46d14c1689047e2e251a0767863a432ca92b2d4d`;
the environment files retain the binary build metadata. Go is **1.27.3**;
Java is 17, Kafka/Kafka Streams 3.9.1, hosted CPU EPYC 7763.

The uninstrumented comparison has one million warmup plus one million measured
records, five fresh-process paired trials per workload. All twenty samples
verified every output, checksums and actual group offsets.

| Workload | Median paired Go/Kafka throughput | Range | Go/Kafka median records/s | Go/Kafka median peak RSS KiB |
|---|---:|---:|---:|---:|
| Filter | 2.04x | 1.11–2.06x | 387109 / 195357 | 266372 / 154552 |
| Count | 6.09x | 5.62–6.40x | 396369 / 62756 | 270428 / 225652 |

Filter still misses the 2.5x target, and Go peak RSS is higher in both cases.
This is a new toolchain baseline, not proof of a compiler-induced improvement:
shared host load, JVM behavior and trial variability remain uncontrolled. One
broker, replication factor one, 64 keys, unequal commit/cache policies and no
common hard native-memory cap prevent production qualification.

The separate diagnostic mode runs one Go case per workload with profiling on.
It retains CPU/heap profiles in the original archive, readable cumulative
reports, environment and independently verified samples. No comparative ratios
are calculated from instrumented timings. Profiles include startup, warmup,
measurement and shutdown. Heap profiles follow cleanup and GC; cumulative
alloc_space is useful for allocation diagnosis, inuse_space is not peak RSS.

For filter, the sampled allocation estimate is 2659MB over the whole process.
Flat allocations are 667MB (25.1%) in RunGroup, 661MB (24.9%) in Task.Emit and
383MB (14.4%) in groupBackend.CommitTasks. Avoid summing their cumulative
columns, which overlap. CPU samples include allocator/GC work; this supports
investigating slice growth and record assembly, not claiming all broker wait
time has been explained. Each optimization needs its own Go 1.27.3 before/after
and uninstrumented broker checks.

[Comparison data](comparison/summary.json) and [profile scope](profiles/summary.json)
remain separate. Both subdirectories retain manifest, environment, samples and
the complete original archive with all commands/logs. Verified archive SHA256:

- comparison: `96499c8500be50ab41de59ecf029cb8691edb89fe6f76017cc1c124b69bf34b8`
- profiles: `b84d6389f27228ce3855284596eb033375ea43a4d68fd3b768656bfaad74c83c`

**Production qualification remains false; full feature parity remains open.**
