# Exact Kafka input batch segments

Go 1.27.3, Linux/amd64, EPYC 9V74, one Go CPU. Compare the prior growing-slice
routing (source `220adb5d7f8cd8c95bbaa4467aaabbff3d2f54c8`, preserved in the
benchmark) and the new helper in the same executable:

```sh
go test ./adapters/kafka -run '^$' -bench BenchmarkGroupInputAssembly -benchmem -benchtime=300ms -count=5 -cpu=1
```

Both strategies run with three assigned partitions. Input layouts represent a
poll from one partition, contiguous records across three partitions, or three
interleaved partitions. Every timed iteration checks strict per-partition order,
unique in-range offsets, routing, all record values/timestamps, total count and
complete checkpoints. Thus time includes this same linear validation on both
sides. Task factory/cache work, broker/client polling and durable commits are
excluded. The worker now also avoids repeated task cache lookup per record;
that additional runtime change is not measured by this isolated benchmark.

| Records | Layout | Before ns/op | After ns/op | After/before | B/op before → after | allocs/op before → after |
|---|---|---:|---:|---:|---:|---:|
| 1 | single | 358.4 | 360.1 | 1.005 | 640 → 576 | 6 → 6 |
| 1 | contiguous | 370.3 | 349.0 | 0.942 | 640 → 576 | 6 → 6 |
| 1 | interleaved | 355.3 | 347.3 | 0.977 | 640 → 576 | 6 → 6 |
| 256 | single | 24457.0 | 18280.0 | 0.747 | 49552 → 25056 | 14 → 6 |
| 256 | contiguous | 33585.0 | 19196.0 | 0.572 | 74352 → 25632 | 33 → 10 |
| 256 | interleaved | 32471.0 | 19382.0 | 0.597 | 74352 → 25632 | 33 → 10 |
| 4096 | single | 709051.0 | 325977.0 | 0.460 | 1450384 → 360928 | 21 → 6 |
| 4096 | contiguous | 550604.0 | 347447.0 | 0.631 | 909936 → 361504 | 45 → 10 |
| 4096 | interleaved | 536905.0 | 335416.0 | 0.625 | 909936 → 361504 | 45 → 10 |

All checks passed. Full single-partition polls allocate 75.1% fewer bytes;
three-partition polls allocate 60.3% fewer. Median assembly time falls by 54.0%
and about 37% respectively. The single-record/single-partition median is 0.5%
slower, within short-run variation; do not advertise a universal speedup.
Five sequential samples provide regression screening, not a controlled-host
significance claim. Task-segment capacity, null/empty records, multi-topic order
and rejected ownership have focused tests. Arrays are fresh per poll, not pooled.

This is Go input routing, not a Kafka Streams comparison. Full broker fault,
read-committed output and production performance gates remain independent.

After source SHA256:
- `adapters/kafka/group_input.go`: `33cea814cdfdf0f3dce68651fe33e438f8c2e4b05aebb11f71653e88012f90a0`
- `adapters/kafka/group_worker.go`: `2b4be7243f78c474d2a4df9a3784d481a2db7168530a8fff65ac7ba4308fe95c`
- `adapters/kafka/group_input_bench_test.go`: `a2f797c7906fa213c3c5664ccbc7ac0908d903bba0d49c444e84065c8888748f`
