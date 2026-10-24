# Direct store restoration evaluation

This measures the same Pebble cache loads with a streamed restore capability and
with a wrapper that forces the legacy intermediate image. It is a local
restoration-path comparison, **not Kafka broker throughput or large-state qualification**.

| Entries | SyncWrites | Image median ms | Stream median ms | Image / stream allocated bytes per restore |
|---:|---|---:|---:|---:|
| 1,024 | false | 0.670 | 0.280 | 409,973 / 124,000 |
| 1,024 | true | 0.714 | 0.415 | 354,538 / 69,626 |
| 32,768 | false | 43.488 | 9.631 | 10,114,058 / 1,794,842 |
| 32,768 | true | 41.271 | 13.104 | 9,005,770 / 611,506 |

The larger synchronous cache load used about 93% fewer allocated bytes and took
about 3.15x less time. These measurements include both the removed image and
different delivery order: the model changelog has sorted insertion keys, whereas
the legacy image is traversed in unspecified map order. The time difference
cannot be attributed solely to allocations. Background engine work can also
change allocations observed within a short measurement interval.

The [benchmark source](../../../adapters/pebble/restore_bench_test.go) creates
the model log and opens an engine outside timing. Each timed iteration resets
the namespace, constructs/restores a task, verifies a representative value and
checkpoint, then closes the task. Both paths apply 256-record batches. Five
samples each use three iterations, GOMAXPROCS=1, 64-byte values and identical
SyncWrites settings. Engine reopen, Kafka fetch, failure recovery and network
costs are absent. SyncWrites calls the engine's sync path on the recorded
temporary filesystem; power-loss durability was not tested here.

```sh
GOMAXPROCS=1 go test ./adapters/pebble -run '^$' \
  -bench BenchmarkRestoreIntoPebble -benchmem -benchtime=3x -count=5
```

[samples.txt](samples.txt), [summary.json](summary.json),
[environment.json](environment.json) and [engine-log.txt](engine-log.txt) retain
the raw run. Samples use a shared virtual host, no CPU pinning and sequential
workload order. They do not prove a hard RSS bound or larger-than-RAM behavior.
The reference log itself retains all mutations in memory outside timing.

Independent correctness tests cover complete state, update/delete/empty ordering,
sink failures, cancellation, unpublished-store cleanup, replay, and the real Kafka
adapter's bounded batches. Task stores that retain all data in RAM and window
expiry heaps remain proportional to state.
