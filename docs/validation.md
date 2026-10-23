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

The reference backend is an in-memory model. There is **no production Kafka
adapter**, broker-backed changelog, consumer-group assignment, actual distributed
repartition exchange or crash-safe local store yet. Consequently the project
makes no broker-level exactly-once, rebalance-safety, production throughput, or
full Kafka Streams compatibility claim. Broker failure injection, compaction
restore, real repartitioning and representative live scheduling are release gates.

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
