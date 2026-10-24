# Explicit 1 MiB fetch: paired Kafka follow-up

Source `0729a01c251aa1345e40d245b92aa9b0753af8b2` passed
[all verified comparison/profile cases](https://github.com/SoraDaibu/kstream-go/actions/runs/40097099153)
plus ordinary, 864 reference, persistent-state and four broker fault suites.
Go 1.27.3, Java 17, Kafka/Kafka Streams 3.9.1, EPYC 9V45, 1M warmup and 1M
measured inputs, five fresh-process alternating pairs per workload.

The benchmark explicitly uses a 1 MiB Go fetch target selected by the separate
same-host diagnostic. Output capacity is 4096. Library/app/script defaults stay
8 MiB. Manifests retain both settings and the target's oversized-batch limitation.

| Workload | Median paired Go/Kafka throughput | All paired ratios | Go/Kafka median records/s | Go/Kafka median peak RSS KiB |
|---|---:|---|---:|---:|
| Filter | 2.58x | 1.17, 2.59, 2.41, 2.77, 2.58 | 612486 / 252857 | 65576 / 156968 |
| Count | 4.97x | 5.64, 5.86, 4.59, 4.97, 4.30 | 599038 / 115854 | 69152 / 235964 |

Both predeclared paired medians exceed 2.5x in these two fixtures. Filter's wide
range includes a 1.17x trial, so this does not establish a universal or production
2.5x result. Ratios are medians of paired rates, not ratios of separate absolute
medians. Keep every trial rather than selecting the faster ones.

Median p99 receipt latency is Go/Kafka 942/3228ms for filter and 1094/7911ms for
count. Approximate observed CPU is 1000/2980ms and 1130/5920ms. Go RSS is now
lower in both cases. The same-host diagnostic supports the fetch target's memory
effect; independent hosted runs cannot attribute all throughput changes to it.
Count's paired ratio is lower than the preceding 8 MiB run's 5.95x.

All samples check exact output order/count/checksum, extra tails and actual
committed group offsets. Average Go inputs per transaction are about 3846,
versus about 4032 in the preceding run. Different commit/cache policies, shared
CPUs, one broker/RF1, tiny state/values and no common hard RSS cap remain limits.
Broader values, joins/windows, HA/recovery and controlled-host measurements are
still required. Full feature parity is also incomplete.

Complete archives retain commands/logs and diagnostic raw profiles, kept separate
from uninstrumented comparison timings. SHA256 verified against Actions metadata:

- comparison: `428c274e07045b3617d1f307d239529561ab8a2c04e77758981fb0cc8c195843`
- profiles: `49f7d0d9c2f099049dcb8d498a43e1142c95789d565e092d6291f4520ce0750c`
