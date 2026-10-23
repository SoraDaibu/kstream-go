# kstream-go

An experimental, independent Go stream-processing core using Apache Kafka
Streams as its conceptual and semantic reference. Apache-2.0 licensed.

**This is an architecture and executable-core prototype, not a production Kafka
Streams replacement.** A Kafka broker adapter, distributed repartitioning,
consumer-group assignment and crash-safe persistent stores are not implemented.
The implemented oracle scenarios pass differential testing against Kafka
Streams 3.9.1; broader compatibility remains unproven. See the
[compatibility matrix](docs/compatibility.md) and [validation status](docs/validation.md).

The implementation keeps typed operators synchronous inside serial partition
tasks. It separates the public DSL, typed graph compiler, Processor API, logical plan, execution,
state and infrastructure adapters. No Java source or class hierarchy is ported.

```go
b := stream.NewStreamsBuilder()
words := stream.Stream(b, "words", serde.String, serde.String)
counts := stream.Count(words, "counts")
counts.ToStream().To("counts-out")
topology, err := b.Build()
```

Go does not support methods with additional type parameters. Type-changing
operations use package functions such as `stream.Map` and `stream.Join`;
type-preserving operations use methods. Null keys and values have explicit flags
in `record.Record[K,V]`; Go zero values are ordinary data. Source/sink/store
boundaries use explicit codecs. Build freezes the builder.

## Run

Requires the Go **programming-language** toolchain, version 1.23 or newer.
The core uses only the standard library.

```sh
go run ./examples/wordcount
./scripts/validate.sh
```

The complete example instantiates an in-memory transaction backend, creates a
partition task, feeds records, and reads output. It prints `go: 1`, `kafka: 1`,
`go: 2` after the topology description. A production service cannot yet connect
this core directly to Kafka.

## Included behavior

- `KStream`: filter/filterNot, map/mapValues, flatMap/flatMapValues, selectKey,
  merge, observers, typed custom processors and sinks.
- `KTable`: source upserts, tombstone deletes, grouped counts and stream-triggered
  inner joins against current table state.
- Event-time tumbling counts with half-open windows, grace, aggregate timestamps,
  operator-local stream-time, and retention of open windows.
- Partition tasks with independent state, committed changelog recovery, offset
  replay suppression and generation fencing in the in-memory reference backend.
- Atomic state/output/offset transaction contract and fail-closed recovery after
  errors or ambiguous commit outcomes. External callback effects are outside it.
- Bounded-worker and goroutine-per-task finite-batch schedulers and comparative
  benchmarks. No production winner is asserted before measurement.

Key-changing maps/processors require repartitioning before key-local operations.
An explicit `Repartition` node is represented logically, and Build rejects it
until a broker adapter can implement an actual exchange. This avoids silently
producing incorrect distributed aggregates. Callbacks must be deterministic,
respect record immutability, and use task stores for recoverable state.

## Qualification program

Full Kafka Streams parity and a 2.5x production-throughput target are tracked in
[the feature inventory](docs/roadmap/parity.md), [qualification rules](docs/roadmap/qualification.md)
and [issue #3](https://github.com/SoraDaibu/kstream-go/issues/3). These targets are
not achieved by the initial prototype. Exploratory comparisons and all remaining
gaps remain visible. Package-layer rules are checked automatically in CI.

## Design and validation

Read the [architecture](docs/architecture.md) and the required ADRs:

1. [Kafka Streams compatibility boundaries](docs/adr/0001-compatibility.md)
2. [Public Go API and generics](docs/adr/0002-go-api.md)
3. [Task scheduling and concurrency](docs/adr/0003-scheduling.md)
4. [State durability and transaction coordination](docs/adr/0004-durability.md)
5. [Topology compilation and optimization](docs/adr/0005-compilation.md)

`go test -race ./...` exercises semantic boundaries and recovery. With Java 17,
Maven and Python 3 installed, `./scripts/differential.sh` compares exact output
traces against Kafka Streams **3.9.1** using its public TopologyTestDriver. See
[integration details](integration/README.md). The differential suite is a
correctness gate, not evidence of broker-level exactly-once behavior.

See [benchmark methodology](docs/benchmarks/README.md) for comparing schedulers.
The architecture remains provisional for production until independent semantics,
representative performance and broker failure gates pass.

Contributions follow [CONTRIBUTING.md](CONTRIBUTING.md). Apache Kafka and Kafka
Streams are ASF trademarks; this project is independent of the ASF.
