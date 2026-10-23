# Scheduling benchmarks

ADR 0003 is provisional. The [initial measured evaluation](2026-10-23/README.md)
includes raw output and environment details. Its results vary with batch size;
no production scheduling winner is claimed.

```sh
go version
lscpu
GOMAXPROCS=1 go test ./execution -run '^$' -bench BenchmarkSchedule -benchmem -count 5
GOMAXPROCS=4 go test ./execution -run '^$' -bench BenchmarkSchedule -benchmem -count 5
```

The matrix covers 8/256 tasks, 1/256 records per batch, zero/128 integer-mixing
steps per record, worker counts 1/GOMAXPROCS, and both schedulers. Each iteration
advances offsets so committed-input replay suppression cannot turn the benchmark
into a no-op. Tasks and records are prepared outside the timed region; queueing,
transaction coordination, offset assignment and scheduling are measured. A
private no-op backend isolates scheduler/transaction overhead from the reference
backend's global mutex. It is explicitly not a durability benchmark.

Record Go version, CPU, GOMAXPROCS, host load, commands and raw output; compare
repeated runs with benchstat. Hosted CI CPU availability is variable. Keep real
Kafka throughput and store benchmarks separate. Add skew, blocked tasks,
backpressure, cancellation, resident memory and p95/p99 record latency before
choosing a production live scheduler. The current finite-batch scheduler does
not establish live-stream fairness or latency.

Run the manual Benchmarks GitHub Actions workflow to collect reproducible raw
artifacts once the repository is published. Do not commit machine-specific
results without their environment and exact command.
