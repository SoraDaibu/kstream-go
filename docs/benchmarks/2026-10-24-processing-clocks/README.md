# Processing clock and transaction descriptor evaluation

Go 1.27.3, Linux amd64, AMD EPYC 9V74. GOMAXPROCS=1, pinned CPU 0, five
alternating before/after pairs, 200 ms per case. Baseline is PR 30's corrected
source (local 1e3e02ab1f11a7c30b5fe664180befd71a1684f6, remote
5ab0e951a4ebf79d5998858d393c84012948adc6). Both runs use the identical existing
BenchmarkTypedProcessorDispatch and BenchmarkWindowExpiry implementations.
These are local dispatch/state checks, with no broker or durable commit I/O.

The [initial candidate](initial/summary.md) added optional clock metadata to the
transaction value. This moved its allocation size class by 16 B per transaction;
typed dispatch stayed at three allocations, and paired timing medians ranged
1.009–1.036 after/before across all eight cases. This regression was retained and
prompted the private descriptor change.

The [final candidate](final/summary.md) embeds the transaction descriptor in its
exclusively owned task. Every commit receives a value copy; offset/output/state
buffers remain freshly owned, and processing clocks use copy-on-write. Existing
retained-output tests and the new full-transaction retention test pin ownership
through subsequent transactions, replay-only calls and close.

Typed dispatch changes from 320 to 256 B/transaction and three to two allocations
for both batch sizes and both APIs. Paired timing medians range 0.929–0.988.
Window cases lose one allocation (21 to 20); their medians range 0.998–1.027,
including regressions. Expiry-window byte counts depend slightly on benchmark
iteration count; inspect all raw rows instead of treating them as fixed sizes.
Five short samples do not establish a stable end-to-end speedup.

[Scoped clock measurements with the final scope IDs](final/scoped-base64.txt) compare one native component with
a second disconnected component, with identical scalar input and clock reads.
Scoped maps cost 512 B/four allocations per transaction versus 256 B/two for the
native path. Median times are about 592.5 vs 356.2 ns for one-record transactions,
and 23,434 vs 17,399 ns for 256-record transactions. This is the explicit cost of
the new capability on this fixture; it is amortized per transaction, with no
additional allocation per record or operator. Runs are sequential by case rather
than alternating, so they are descriptive overhead measurements. The earlier
[JSON-scope measurement](final/scoped.txt) is also retained; final IDs use
length-prefixed source-name bytes and URL-safe base64.

Commands used for ordinary pairs:

```sh
GOMAXPROCS=1 taskset -c 0 go test ./stream -run '^$' \
  -bench 'Benchmark(TypedProcessorDispatch|WindowExpiry)$' \
  -benchmem -benchtime=200ms -count=1
```

The scoped command uses -bench BenchmarkProcessingClockDispatch$ and -count=5.
All ten raw files for each candidate, environment notes and summaries are kept.
This does not qualify the 2.5× Kafka Streams throughput target.
