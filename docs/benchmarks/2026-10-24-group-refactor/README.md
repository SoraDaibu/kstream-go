# Single-task path after group preparation refactor

The group implementation shares task validation, staging, local apply and
checkpoint publication with the existing independent-task path. This evaluation
checks that refactoring's existing hot path; it does not measure Kafka group
throughput or qualify the production target.

Each iteration processes 1024 records with 64 keys, using an undurable no-op
backend. Records receive fresh offsets so replay suppression cannot make the
work a no-op. Five samples use 1000 iterations per workload and GOMAXPROCS=1.
Allocation counts are identical before/after in all eight workloads.

| Workload | Transaction batch | Before median ns/iteration | After median ns/iteration |
|---|---:|---:|---:|
| Stateless | 1 | 456,234 | 386,989 |
| Stateless | 16 | 208,204 | 187,661 |
| Stateless | 256 | 165,897 | 158,471 |
| Stateless | 1024 | 178,738 | 184,554 |
| Count | 1 | 708,846 | 665,269 |
| Count | 16 | 586,538 | 553,749 |
| Count | 256 | 393,911 | 380,654 |
| Count | 1024 | 348,257 | 346,059 |

The initial stateless/1024 sample was 3.3% slower. A longer follow-up alternated
five before/after trials with 10,000 iterations. Its median after/before time
was 1.013, with individual ratios 0.922–1.074. A small regression remains possible;
these shared-host samples do not justify attributing small timing differences
to code. No speedup claim is made for this refactor. The functional gain is the
explicit multi-task atomic boundary with unchanged allocation counts on the
existing path. Controlled end-to-end qualification is still required.

```sh
GOMAXPROCS=1 go test ./execution -run '^$' \
  -bench BenchmarkTransactionBatch -benchmem -benchtime=1000x -count=5
```

The follow-up uses compiled test binaries from each source tree with
`-test.bench='^BenchmarkTransactionBatch$/^stateful=false$/^batch=1024$'`,
`-test.benchtime=10000x`, `-test.count=1` and `-test.benchmem`, alternating order.

[before.txt](before.txt), [after.txt](after.txt), [followup.json](followup.json),
[summary.json](summary.json) and [environment.json](environment.json) retain
the complete measurements. Baseline remote source is
`453a1de3cee4256abf5781db4e08b438227b9a9a`; candidate execution source hashes are
recorded in the environment. There is no CPU pinning, broker, persistence,
latency distribution or RSS comparison in this experiment.
