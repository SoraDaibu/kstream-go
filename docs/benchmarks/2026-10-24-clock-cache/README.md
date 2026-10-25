# Scoped clock cache evaluation

Baseline: main at 4053228d7354b8526b123e1684fa07d01a334088 (local equivalent
8e298b054c84875a53cb59b8506d215146ca58e1). Go 1.27.3, Linux amd64, EPYC 9V74,
GOMAXPROCS=1 and CPU 0. All results below are local runtime experiments without
Kafka, durable I/O or a production performance qualification.

The original scope observer reads and updates a map per record; processors read
the same map again for StreamTime. The candidate keeps a task-owned scalar view
and materializes advancing scopes into one complete checkpoint after successful
staging. Graph compilation delegates clock ownership to execution. The public
handle shares private state even when copied; the private state embeds its handle
so each scope still needs one state/handle allocation. The ordinary Task keeps
its original size and field offsets through an optional cache pointer.

## Predeclared acceptance criteria

| Metric | Limit |
|---|---|
| Scoped dispatch, 256-record transaction | Median time after/before ≤0.85 |
| Ordinary dispatch, window and pipeline controls | Every case's median after/before ≤1.05 |
| Scoped and typed dispatch bytes/allocations per transaction | Unchanged |
| Setup time and allocated bytes, 1/2/32/256 scopes | Median after/before ≤1.15 |
| Correctness and ownership | Race/vet/layers, all differential and broker gates; copied views, rollback, epoch wrap and retained maps |
| Public API and checkpoint encoding | Unchanged |

Use five alternating whole before/after phases. The final private-cache series
uses one-second processing samples and 200ms setup samples. Setup is measured
separately so persistent cache memory cannot be hidden outside a dispatch timer.
Ratios use the median of five paired ratios; this can differ from the ratio of
the separate absolute medians. All individual samples remain available.

Result: **rejected; production code is unchanged**. The final candidate reaches
0.709 median time after/before for scoped 256-record dispatch (29.1% less time;
all paired ratios 0.656–0.730). Dispatch remains 512 B/four allocations per
transaction. The 4096-key long-window control records **1.053066**, exceeding
the **1.05** ceiling. Do not round this into a passing result.

The ordinary single-stage task's setup stays at 17,800 B/21 allocations. At 256
scopes, setup changes from 242,096 to 252,360 B (+4.24%) and adds one cache-object
allocation, with a 1.063 setup time ratio. These tradeoffs meet their setup limits;
they do not cancel the failed window-control criterion.

[All final results](private/summary.json) and rejected candidate source files are
retained. Public API copying, ownership and checkpoint regression tests remain
useful and pass on the unchanged production implementation. A future attempt
needs another predeclared experiment; there is no production speedup in this PR.

Recompute the numerical decision with:

```sh
python3 docs/benchmarks/2026-10-24-clock-cache/evaluate.py
```

The command intentionally exits with status 1 for this rejected candidate.
The evaluator checks all 24 cases and emits every ratio and failed limit. It
does not certify correctness gates or broker throughput. Manifests include
source hashes, environment, phase order and limits. Pipeline state/expiry bytes
can vary slightly with iteration count; fixed dispatch allocations are checked
exactly.

## Rejected candidates and limits

- [Tiny clone experiment](rejected-clone/summary.json): combining key/value
  payloads up to 16 bytes saved one allocation but increased the small clone
  time medians by roughly 16–37%. The larger method exceeded Go's inline budget;
  a helper revision still failed screening. Some pipeline controls also exceeded
  5%. Both source variants, all five paired runs and the helper screening run are
  retained. Production record.Clone remains unchanged. New tests cover 121
  null/empty/size combinations, independent writable copies and overlapping inputs.
- [Preliminary cache series](preliminary/summary.json): its first phase overlapped
  local validation, so it is retained but excluded from the acceptance decision.
- [First isolated cache](rejected-inline-cache/summary.json): scoped dispatch
  improved, but the same-type single-record control regressed by 7%, failing
  the gate. Mutable fields inside the public handle also failed a copied-view
  regression added during review.
- [Cold-field layout](rejected-cold-layout/summary.json): preserving ordinary
  field offsets alone still failed several controls. The final candidate moves
  all optional cache state behind one pointer and keeps mutable clock state
  behind the public handle.

Longer samples address the short-control uncertainty, but a shared runner still
has noise and wide individual ranges. Report modest control differences as
measurements, not causal speedups. This targets scoped processor dispatch; the
existing broker filter/count topologies do not use this path, and no new 2.5×
Kafka Streams claim follows from this experiment. See [ADR 0028](../../adr/0028-scoped-clock-cache.md).
