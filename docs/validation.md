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
supports explicit assignment and an experimental bounded group worker with
transactional output/state/input positions, broker fencing and read-committed
restoration. Group restart, rebalance during staging and an open-transaction
process kill have broker coverage. Distributed exchanges, reusable local recovery
checkpoints, full group fault qualification, forced compaction and production
performance remain release gates.

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

## Streamed restoration cycle

Commit `453a1de3cee4256abf5781db4e08b438227b9a9a` passed
[Go/race/layer and differential CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40052853772),
[persistent-state integration](https://github.com/SoraDaibu/kstream-go/actions/runs/40052853826),
[broker recovery](https://github.com/SoraDaibu/kstream-go/actions/runs/40052853756),
and exploratory checksum validation. The broker suite forced seven-record restore
batches, injected a sink failure, and compared streamed state/checkpoints with
the legacy image path. Local tests cover cancellation, borrowed buffers, partial
restore cleanup, setup panic cleanup and rejecting an unbuilt topology before
fencing a healthy owner. The [local cache-load measurements](benchmarks/2026-10-24-streamed-restore/README.md)
retain raw allocation/time samples and limits; larger-than-RAM qualification and
local checkpoint reuse remain open.

## Atomic task-group cycle

Commit `804618438c05785e63f8a0d81948313e9aff3323` passed
[Go/race/layer and differential CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40054483754),
[persistent-state integration](https://github.com/SoraDaibu/kstream-go/actions/runs/40054483760),
[fixed-assignment broker recovery](https://github.com/SoraDaibu/kstream-go/actions/runs/40054483748),
and exploratory checksum validation. Thirteen oracle scenarios now run at three
batch sizes with both stores and both independent/group task commits: 156 exact
comparisons. Model failures cover stale last-participant generations, aborted and
ambiguous durable decisions, local-apply failure, callback panic/error, routing,
membership and concurrent ownership. The [single-task refactor measurements](benchmarks/2026-10-24-group-refactor/README.md)
record unchanged allocations and a small possible timing regression with a longer
paired follow-up. This model boundary alone does not certify Kafka group EOS.

## Experimental Kafka group-worker cycle

Commit `01c52db6e56c2d287fc2be1731bf0c7a14917715` passed the four isolated
[Kafka 3.9.1 broker suites](https://github.com/SoraDaibu/kstream-go/actions/runs/40056468708):
fixed-assignment recovery, group restart with committed-offset checks,
a second member joining during staged processing, and a subprocess killed after
producing state/output but before transaction end. The killed worker's records
remain invisible and fresh processing resumes within the 20-second recovery gate.

The kill test first found a producer initialization failure during Kafka's old
epoch abort. Initialization now uses the public ProducerID API and retries
explicitly retryable startup failures before any input is polled or produced.
No ambiguous data transaction is retried in place. The test also preserves the
same range assignment protocol across the killed and replacement processes.

These are focused single-broker fixtures. They do not certify all commit phases,
network ambiguity, coordinator failover, forced compaction, cooperative or
heterogeneous assignment, or Kafka Streams production throughput. See
[group-worker limits](../adapters/kafka/GROUPS.md) and the parity inventory.

## First broker comparison cycle

Source `b200080febd82e1508fcb7d7ee40f80d22b8078d` passed ordinary CI,
all four broker fault suites, persistent-state integration, driver checksums and
the [first paired broker workflow](https://github.com/SoraDaibu/kstream-go/actions/runs/40058301396).
Twenty samples verified ordered per-key outputs, checksums and actual group offsets.
The [retained results](../benchmarks/results/2026-10-24-broker-first/README.md)
report a 6.90x median paired ratio for count and 2.06x for filter, with lower
observed p99/RSS in both. Filter remains below the target, and the experiment's
single-broker/shared-host limits prevent production qualification.

## Stream-table nullable join cycle

Source `9efa5833c87938f5dfbd8932cefeb881c4bb8829` passed
[Go/race/layer and independent differential CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40061456949).
Fifteen scenarios now compare 4,677 input rows at three batch sizes with both
stores and both independent/group commits: 180 exact output traces. New scenarios
cover nullable inner results and left joins, including null stream keys, missing
table entries, empty values and table deletes. Focused Go tests also cover source
replay/recovery and invalid plans. Versioned lookup and cross-input Kafka runtime
scheduling remain open gates.

## Hopping-window cycle

Source `32ca9742cc79cc135703742e51956e70f488fecf` passed
[Go/race/layer and independent differential CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40061776072).
Seventeen scenarios compare 5,313 inputs at three batch sizes with memory/Pebble
and independent/group transactions: 204 exact traces. Hopping advances 5 and 6
for size 10 validate output order, overlap, grace boundaries and null-valued
input. Local tests cover bounded arithmetic, replay and restoration after an
aborted multi-window batch with both stores. Existing tumbling allocations stay
unchanged in the retained [before/after samples](benchmarks/2026-10-24-hopping-refactor/README.md).

## Table transforms and Go 1.27 cycle

Source `54155a7bc2b34e20d5067f22f2d5359d760ba55b` passed
[Go/race/layer and differential CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40075665208).
Twenty-one scenarios compare 6,585 input records across both stores, all three
batch sizes and both transaction modes: 252 exact traces. The first run found
redundant Go tombstones for already absent filtered rows. The implementation was
corrected; a seven-row fixture and focused test preserve the reference behavior.
Nullable table mapping and chained transformed-table lookup also match.

Source `c0d66893fd12e76db1d61bfdc690dc3e7bbde90e` repeated the
[correctness/oracle suite on Go 1.27.3](https://github.com/SoraDaibu/kstream-go/actions/runs/40075877906),
with [persistent-state tests](https://github.com/SoraDaibu/kstream-go/actions/runs/40075877956)
and all four [broker recovery/fault suites](https://github.com/SoraDaibu/kstream-go/actions/runs/40075877912)
passing. Historical Go 1.23 measurements remain historical; new performance
conclusions require the [current toolchain baseline](toolchain.md).

## Processor source metadata cycle

Source `6eddd0c8b8fc2bdfb660a48b5d4c185d9fa9d136` passed
[Go/race/layer and differential CI](https://github.com/SoraDaibu/kstream-go/actions/runs/40080734208),
[persistent state](https://github.com/SoraDaibu/kstream-go/actions/runs/40080734196),
all four [broker fault suites](https://github.com/SoraDaibu/kstream-go/actions/runs/40080734187)
and verified broker/profile workloads. Twenty-three scenarios compare 7,205
inputs across stores, three batch sizes and both commit modes: 276 exact traces.
New Processor API scenarios check source topic/partition/offset through merged
topics and fanout. Go tests cover nonzero partitions, replay, initialization,
commit scope and cleanup after errors, panics and owner exit.

Metadata capture uses setup wrappers only for custom processors; raw runtime
handlers opt in. A rejected slice-reference design added one per-record
allocation. The final design preserves allocation counts, with retained
[paired overhead measurements](benchmarks/2026-10-24-processor-metadata/README.md).

## Explicit output capacity cycle

Source `58a50254fb02f8bc5eaa59aacd90becce750fe42` passed
[Go/race/layer and all 552 reference comparisons](https://github.com/SoraDaibu/kstream-go/actions/runs/40083138116),
[persistent state](https://github.com/SoraDaibu/kstream-go/actions/runs/40083138150),
all four [broker fault suites](https://github.com/SoraDaibu/kstream-go/actions/runs/40083138120)
and verified comparison/profile workloads. The 23 scenarios and 7,205 inputs
are unchanged; output capacity 0 and 256 now run in every batch/store/commit
configuration. Both settings preserve exact reference output.

[Retained broker evidence](../benchmarks/results/2026-10-24-output-capacity/README.md)
shows lower sampled output-array allocation, while filter remains below the
2.5x throughput target and peak Go RSS remains higher. Default output capacity
stays zero; application-specific hints have documented sparse/fanout tradeoffs.

## Cross-type Processor API cycle

Source `bf1c22cf6dde49e26eb1ce026b07f2ffcb0719ec` passed
[Go/race/layer and all 600 reference comparisons](https://github.com/SoraDaibu/kstream-go/actions/runs/40084028574),
[persistent state](https://github.com/SoraDaibu/kstream-go/actions/runs/40084028579)
and all four [broker fault suites](https://github.com/SoraDaibu/kstream-go/actions/runs/40084028587).
Twenty-five scenarios compare 7,833 inputs across both stores, three batch sizes,
two commit modes and output capacities 0/256. Two public Java ContextualProcessor
traces check numeric intermediate keys/values, UTF-8 byte lengths, source offsets,
nulls, dropped records and fanout with modified timestamps. All preceding input
traces remain unchanged. Local tests additionally cover staged custom state,
task/supplier ownership, recovery/replay, and state/output/offset rollback.

[Dispatch measurements](benchmarks/2026-10-24-typed-processors/README.md) find the
same transaction allocations for same-type and cross-type APIs at 1/256 records;
this is an overhead check, not a Kafka throughput qualification.

## Ordered branching cycle

Source `8f476c6deb728739342f77351cea0f7689c07fb0` passed
[Go/race/layer and 648 reference comparisons](https://github.com/SoraDaibu/kstream-go/actions/runs/40084431698),
[persistent state](https://github.com/SoraDaibu/kstream-go/actions/runs/40084431674),
all four [broker fault suites](https://github.com/SoraDaibu/kstream-go/actions/runs/40084431695)
and verified comparison/profile workloads. Twenty-seven scenarios contain 8,453
input rows. The new Java public split/branch scenarios check overlapping
predicates, first-match order, null/empty input and dropped/catch-all routing.
Go tests additionally verify unmatched offset progress across recovery and
transaction rollback spanning different selected branches.

## Windowed reduce cycle

Source `908680e75a4f99703a53a04242122980f4e94ce4` passed
[Go/race/layer and all 720 reference comparisons](https://github.com/SoraDaibu/kstream-go/actions/runs/40085730734),
[persistent state](https://github.com/SoraDaibu/kstream-go/actions/runs/40085730760)
and all four [broker fault suites](https://github.com/SoraDaibu/kstream-go/actions/runs/40085730740).
Thirty scenarios compare 9,443 inputs. Three windowed-reduce traces add tumbling,
divisible/uneven hopping windows, nullable inputs/results, aggregate timestamp
lifetimes and grace boundaries across all store/batch/commit/capacity settings.

The first reference run found that windowed reduce accepts null values: a first
null emits a tombstone and advances the clock; an existing aggregate passes null
to the reducer. The implementation/API were corrected with an explicit input-null
flag and a [six-row regression fixture](../integration/fixtures/window-reduce-null-input.json).
Null keys still skip without advancing the clock. This deliberately preserves
Kafka's observed difference from unwindowed reduce rather than assuming shared
null policy across the DSL. Go tests additionally cover recovery of a clock whose
null-valued aggregate row is absent, aborted fanout, and bounded derived indexes.

[Count regression samples](benchmarks/2026-10-24-windowed-reduce/README.md)
retain the rejected shared-lifecycle timing regression and final clock-cache
revision. Count's state format is unchanged; its benchmark removes one allocation
per input after caching the operator-owned durable clock.
