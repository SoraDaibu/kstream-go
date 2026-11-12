# Qualification targets

The project goals are feature parity with Apache Kafka Streams, materially better
performance, and a maintainable Go architecture. These are acceptance criteria,
not claims about the current implementation.

## Reference and scope

Kafka Streams 3.9.1 is the initial pinned executable reference because the existing
oracle uses it. The parity inventory includes public DSL/Processor/runtime/state
capabilities, not Java package names or classes. A newer Kafka reference must be
added before claiming parity with that version. Unsupported items remain visible;
one selected scenario passing does not certify its entire feature family.

## Performance threshold

Interpret "150% better performance" as at least **2.5 times throughput**, not 1.5
times. Qualification requires correct equivalent outputs, equal processing and
transaction guarantees, comparable state-store durability, identical input,
partition counts, broker configuration and resource budgets. Measure end-to-end
committed output throughput, p50/p95/p99 latency, CPU time, allocation/GC behavior,
peak RSS and recovery duration. Record warmup, trial length, hardware, versions,
configuration, input seed and raw per-trial measurements. Alternate run order;
use at least five independent trials and confidence intervals on a stable host.

Include stateless transforms, keyed aggregation with uniform and skewed keys,
stream/table and windowed joins, window aggregation, repartitioning, recovery,
and exactly-once broker workloads. Report every qualified workload, including
regressions. The 2.5x target must hold per required workload with p99 latency and
memory no worse than the reference at the matched operating point. A different
tradeoff requires an explicit recorded decision; do not average away failures.

Microbenchmarks, TopologyTestDriver throughput, no-op backends and memory-only
stores are exploratory evidence. They cannot qualify production Kafka-backed
performance against a persistent or transactional reference. The performance
status remains NOT QUALIFIED until the full comparison and correctness gates pass.

## Engineering constraints

Keep DSL, topology, processor execution, state and infrastructure independently
understandable. No goroutine/channel per operator; task state has one owner.
Benchmark concurrency choices. Follow public semantics and established designs
from Kafka Streams, franz-go, Pebble and other mature Go systems; cite the design
lesson and independently implement it rather than copying internals. Any new
runtime dependency needs a clear adapter boundary and license compatibility.

## Iteration protocol

For each change: select a missing capability or measured bottleneck; specify its
semantics and failure behavior; implement it at the appropriate layer; add an
independent reference or property/failure test; run race and differential checks;
measure equivalent workloads; retain raw evidence; update the inventory. Merge
only passing changes. Never mark a requirement complete from a roadmap entry,
a workflow definition, a relative-to-old-Go speedup, or an unexecuted test.

## Regression CI

`Performance regression / Benchmark regression` compares the PR merge result
with its base commit (main pushes compare with the previous main commit). Both
revisions run on the same runner, pinned to the same CPU, using Go 1.27.3,
GOMAXPROCS=1 and GOMEMLIMIT=512MiB. Five fresh-process paired trials alternate
base/head order; both binaries use **the base revision's test files** so removing
or weakening a candidate benchmark cannot hide a regression.

The current controls cover record cloning, transaction batching, source metadata,
typed and scoped processor dispatch, clock setup, branch routing and window
expiry/reduce. Each case must satisfy all of these limits independently:

- Median paired time ratio (head/base) <=1.05, the existing clock-cache control budget.
- Median B/op <=1.01 times base; the margin accounts for calibration and rounding.
- Median allocs/op <=base, with no extra allocations allowed. Zero B/op stays zero.

Missing cases, incomplete trials, invalid values, build errors and failed
benchmark assertions fail the job. Raw outputs, source and binary hashes,
environment, trial order and the numerical decision are uploaded even on failure.
Recompute a retained decision with `python3 scripts/benchmark_regression.py RESULTS`.
Run locally with `python3 scripts/run-benchmark-regression.py --base BASE_SHA`.
The output directory must be empty so a later run cannot overwrite evidence.

These controls screen the covered Go paths for regressions within explicit noise
budgets. They do not establish Kafka Streams parity, broker throughput, tail
latency, recovery or production qualification; issue #3 remains open until its
full acceptance criteria pass. Existing race, differential, storage and real
Kafka transaction checks remain necessary. Configure the benchmark job and the
existing correctness jobs as required checks in main's protection rules to
prevent merges on a failed or missing check.
