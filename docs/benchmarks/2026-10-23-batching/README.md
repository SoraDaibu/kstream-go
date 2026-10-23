# Batched transaction staging evaluation

The optimized source is included in commit
`38f96e31a8b1e8364f06c5bc83e22e6972d0efe9`. The map-only baseline runtime is
retained verbatim in [baseline-task.go.txt](baseline-task.go.txt), SHA-256
`4431ca59145f897a94ba0f2801a0ee8a0a764997c46aa3fddbeba68e24580ab7`.
Both variants use the same BenchmarkTransactionBatch in execution/batch_bench_test.go.

Five alternating baseline/optimized trials ran on the same local host with
Go 1.23.12 and GOMAXPROCS=1. Each test binary was compiled separately; run order
alternated each trial. Exact per-trial flags:

```sh
-test.run=^$ -test.bench='BenchmarkTransactionBatch/stateful=true/batch=(1|1024)$' -test.benchmem -test.benchtime=500ms -test.count=1
```

Raw [baseline](map-only.txt), [optimized](small-index.txt) and
[environment](environment.txt) data are retained. Each op processes 1,024 input
records over 64 keys with count updates and serialized output. The backend is
non-durable and discards committed output after serialization.

| Batch size | Baseline median us/op | Optimized median us/op | Baseline B/op | Optimized B/op |
|---|---|---|---|---|
| 1 | 974.502 | 755.906 | 989,972 | 580,369 |
| 1,024 | 389.419 | 362.924 | 397,433 | 396,276 |

Avoiding a map for small write sets improved the small-transaction case while
retaining indexed lookup for larger batches. The observed medians improved in
both selected cases. This is a Go-runtime optimization comparison, not a Kafka
Streams speedup or a production throughput/latency qualification.

To reproduce the baseline, use an isolated checkout of the optimized source and
replace execution/task.go with the retained baseline file before compiling the
test binary. Do not overwrite an active development checkout. Keep task state,
input, codecs and benchmark code identical between the two binaries.
