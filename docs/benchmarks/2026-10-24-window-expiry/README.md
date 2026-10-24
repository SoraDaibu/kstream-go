# Window expiry bookkeeping evaluation

This compares the previous Go full-scan implementation with the derived expiry
heap. It is **not a Kafka Streams throughput comparison or production qualification**.

| Prefilled keys | Size/grace (ms) | Before median ns/record | After median ns/record | Before/after time |
|---|---:|---:|---:|---:|
| 64 | 1024 | 20,217 | 1,534 | 13.18x |
| 64 | 2^60 | 13,779 | 1,475 | 9.34x |
| 4096 | 1024 | 568,483 | 1,991 | 285.53x |
| 4096 | 2^60 | 857,293 | 1,407 | 609.31x |

The large ratios expose a poor baseline: it cloned all retained entries into a
map on every clock advance, even when none could expire. At 4096 keys with the
large window it allocated about 853 KB and 4232 objects per record; the new path
allocated 894 bytes and 22 objects. The index still consumes O(N) retained heap
memory and scans state during restoration. Allocation rate does not establish
resident-memory or recovery-memory performance.

Both implementations use the same checksum-verified benchmark in
[window_bench_test.go](../../../stream/window_bench_test.go), 5000 timed records
after one initial record per key, a monotonic millisecond clock, five samples,
and GOMAXPROCS=1. Size and grace are equal. The 1024ms workload exercises actual
rollover/expiry; the 2^60ms workload retains all keys. Prefill and the independent
checksum model are outside timing. Serialization, output consumption, task
coordination and local state updates are timed. The checksum backend has no
broker or durability.

Exact command, run separately before and after on the same host:

```sh
GOMAXPROCS=1 go test ./stream -run '^$' -bench BenchmarkWindowExpiry \
  -benchmem -benchtime=5000x -count=5
```

[before.txt](before.txt), [after.txt](after.txt), [summary.json](summary.json) and
[environment.json](environment.json) retain measurements and context. The
[baseline source](baseline-window.go.txt) is the old operator from remote commit
`d309947c199e0f0b0a5959eafce23b39083b01d0` (local equivalent
`906e00dcb23b3efd9ee6102001a2b77f9724e3c6`). It can replace stream/window.go
alongside the same benchmark to reproduce the baseline. Runs were sequential,
without CPU pinning or alternating order; hosted scheduling noise and the short
new-path duration limit precision. Do not interpret the displayed digits as
statistical confidence or extrapolate these ratios to broker/EOS workloads.

Correctness additionally requires the unchanged Kafka Streams oracle, staged
eviction/replay, failed-transaction recovery and unordered-restoration tests.
