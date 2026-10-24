# Kafka group record assembly on Go 1.27.3

Baseline source `878b891345cae2d64fe413b972226e0513246789` (local
`7e620f1`), with the same benchmark file as this change. After: owned array
implementation in this change. Go 1.27.3, Linux/amd64, EPYC 9V74, one Go CPU.
Both phases ran sequentially without another local test/build active:

```sh
go test ./adapters/kafka -run '^$' -bench BenchmarkGroupAssembly -benchmem -benchtime=500ms -count=5 -cpu=1
```

Each case assembles two task checkpoints/mutations and the stated total number
of output records. A synchronous no-network session checks every output value,
partition and timestamp plus total record counts. This measures adapter assembly
only: no Kafka, client buffering, durable commit, local state or result allocation.

| Output records | Median before ns/op | Median after ns/op | After/before | B/op before → after | allocs/op before → after |
|---|---:|---:|---:|---:|---:|
| 2 | 4370 | 4449 | 1.018 | 1992 → 1984 | 40 → 32 |
| 512 | 47629 | 40578 | 0.852 | 92818 → 95898 | 556 → 32 |
| 4096 | 456834 | 376874 | 0.825 | 811750 → 705517 | 4147 → 33 |

All checks passed. At 4,096 outputs, object allocations decrease by 99.2%, bytes
by about 13.1%, and the median assembly time by about 17.5%. The two-output
median is about 1.8% slower, and the 512-output case allocates about 3.3% more
bytes because array allocation size classes differ. These are five sequential
samples, not a controlled-host significance claim.

Distinct-pointer and retained-client-record tests guard null/empty/order and
cross-call ownership. No array is pooled or reused after dispatch. Retaining
one record pointer pins that call's entire struct array, so allocation counts
alone cannot predict application RSS. Real broker/fault and full independent
semantics checks remain required, and this does not establish a Kafka Streams
speedup or production qualification.

After source SHA256:
- `adapters/kafka/group_backend.go`: `daacc4cc59991b407aeec9774aa6651abdcaa5b0d89be208bbf4c8c31275b725`
- `adapters/kafka/group_assembly_bench_test.go`: `676b9b5104e1247b7b3c5037735a723f7f8d60e1352d0a01a1456ba1a30ffff8`
