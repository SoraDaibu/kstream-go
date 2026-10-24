# Validation status

Verified through GitHub Actions on 2026-10-23 at source commit
`fd1761a61aec783393f06a6556f2088d9d434ee7`:

- [Ordinary CI](https://github.com/SoraDaibu/kstream-go/actions/runs/39997157451):
  gofmt check, `go vet ./...`, `go test -race -coverprofile=coverage.out ./...`,
  and the word-count example passed.
- [Bootstrap validation](https://github.com/SoraDaibu/kstream-go/actions/runs/39997157363):
  the same Go checks, the independent Java oracle, and both scheduler benchmark
  strategies passed. Environment: Go 1.23.12, Java 17, Linux/amd64.
- The Kafka Streams **3.9.1** TopologyTestDriver oracle matched exact output
  topic/key/value/timestamp traces for filter, unwindowed count, table updates
  and tombstones, stream-table inner join, and tumbling windowed count. The five
  scenarios contain 1,550 input records in total, including fixed boundaries,
  nulls, out-of-order timestamps and generated rows using seed 73019.
- The first oracle run found that windowed count in Kafka Streams 3.9.1 counts
  null-valued records with present keys, unlike unwindowed count. The Go core was
  corrected; both the trace generator and a focused Go regression test preserve
  this finding. The full differential suite passed afterward.
- [Scheduling measurements](benchmarks/2026-10-23/README.md) include raw output,
  exact commands, environment and an evaluation of both strategies.
- Shell scripts, workflow YAML and embedded shell, Maven XML, trace generation
  and local Markdown links were also checked in the authoring workspace.

The original authoring workspace could not install Go or Maven because shell
network access was blocked. The connected GitHub account and Actions provided
the validation path; the source was formatted by Go's actual gofmt tool. The
temporary bootstrap workflow is removed after collecting these results. Ordinary
CI remains required for subsequent changes.

## Remaining compatibility gates

The memory backend remains a reference model. An experimental Kafka adapter now
supports explicit partition assignment with transactional output/state/input
checkpoints, broker producer fencing and read-committed restoration. It does not
implement consumer-group EOS v2, rebalance-safe assignment, distributed exchanges
or reusable local recovery checkpoints. Full broker fault injection, forced-compaction
validation, group coordination and production performance remain release gates.

All five ADRs preceded implementation. The architecture is a tested experimental
core; production scheduling and additional feature compatibility remain open.

## Transaction-batch and qualification cycle

Commit `38f96e31a8b1e8364f06c5bc83e22e6972d0efe9` passed
[Go/race/layer and differential CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40000342803).
The five oracle scenarios now compare transaction batch sizes 1, 32 and 256.
Additional tests cover batch atomicity, replay, clock order, cross-topic staged
state, ambiguous commits, cancellation and overlay scans/deletes.

The [paired Kafka driver comparison](../benchmarks/results/2026-10-23-driver/README.md)
and [staged-state evaluation](benchmarks/2026-10-23-batching/README.md) retain actual
raw data and limits. Full feature parity and production performance remain
unqualified; issue #3 tracks the complete program.

## Shared compilation and expanded DSL cycle

Commit `7f3955d5253136a230151d138da412aa49a98e9a` passed
[CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40002240280) and the paired
driver workflow. Ten oracle scenarios cover 3,100 source records and compare all
three transaction batch sizes. Shared-node processor factories are instantiated
once per task across merged sources; local race and factory-identity tests pass.

## Real Kafka adapter cycle

Commit `824fe1e425701a8227dd1e817aa7a0a2ac4d139e` passed
[Kafka 3.9.1 integration](https://github.com/SoraDaibu/kstream-go/actions/runs/40003924774):
restart/replay with read-committed isolation, aborted state/output exclusion,
complete multi-topic checkpoints, tombstones versus empty values, producer
fencing, independent partition identities, and source resumption. This is a
single-broker fixture and an explicit-assignment checkpoint protocol, not
consumer-group EOS v2 or a production fault-tolerance qualification.

## Shared persistent-state cycle

Commit `46c3879c56a1e5ca3036d0053e1ca8ef465ad45a` passed
[Go/race/layer and differential CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40051030760),
[persistent-state integration](https://github.com/SoraDaibu/kstream-go/actions/runs/40051030772),
[Kafka recovery with persistent caches](https://github.com/SoraDaibu/kstream-go/actions/runs/40051030770),
and the exploratory comparison's output-checksum gate. Ten oracle scenarios each
run at three batch sizes with both memory and Pebble: 60 exact trace comparisons.
Namespace exclusivity, reopen durability, owned bytes, empty/tombstone values,
ignored read errors, and failed local apply after a successful backend commit
have regression coverage. The full race suite and vet also passed locally.

This adds persistent key-value stores, not a complete state subsystem. Restore
still materializes a full backend snapshot in RAM; streamed restoration, validated
cache checkpoint reuse, specialized stores and full production qualification
remain open.

## Grouped aggregation cycle

Commit `d309947c199e0f0b0a5959eafce23b39083b01d0` passed
[Go/race/layer and differential CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40051651430),
[persistent-state tests](https://github.com/SoraDaibu/kstream-go/actions/runs/40051651394),
and [broker recovery](https://github.com/SoraDaibu/kstream-go/actions/runs/40051651501).
Thirteen oracle scenarios run against both stores at batch sizes 1, 32 and 256:
78 exact comparisons, including reduce, aggregate, nullable initialization,
deletion/reinitialization and aggregate timestamp lifetimes. The expanded
[paired driver comparison](../benchmarks/results/2026-10-24-grouped-driver/README.md)
retains actual raw data and its nonproduction limits.

## Window expiry optimization cycle

Commit `26c4e5da05146cac0935c88073b449f4242ce8cf` passed
[Go/race/layer and all 78 oracle comparisons](https://github.com/SoraDaibu/kstream-go/actions/runs/40052118209),
[persistent-state integration](https://github.com/SoraDaibu/kstream-go/actions/runs/40052118239),
[broker recovery](https://github.com/SoraDaibu/kstream-go/actions/runs/40052118216),
and exploratory benchmark checksum validation. Additional tests cover aborted
eviction batches with both stores, replay and restoration of unordered windows.
The [fixed-input Go before/after evaluation](benchmarks/2026-10-24-window-expiry/README.md)
retains raw data, baseline source, checksum model and limitations. It is separate
from Kafka Streams production throughput qualification.
