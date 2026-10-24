# Produce completion bookkeeping

Go 1.27.3, linux/amd64, EPYC 9V74, one benchmark CPU, five sequential 300ms
samples per case. See [all raw samples](samples.txt).

```sh
go test ./adapters/kafka -run '^$' -bench '^BenchmarkProduceCompletion$' \
  -benchmem -benchtime=300ms -count=5 -cpu=1
```

| Records | Retained-result median ns/call | Error-only median ns/call | Retained/error-only bytes | Allocations, both |
|---|---:|---:|---:|---:|
| 1 | 48.28 | 57.21 | 40 / 64 | 2 |
| 2 | 60.26 | 66.84 | 64 / 64 | 2 |
| 512 | 6343 | 4058 | 13584 / 64 | 2 |
| 4096 | 51508 | 33218 | 98320 / 64 | 2 |

The retained-result model uses the result list/shared callback/wait-group behavior
of franz-go 1.18.1 ProduceSync. Both paths invoke the same fake producer and check
every submission count. Immediate callbacks allow compiler optimizations that a
real network client cannot assume, so these allocation counts and timings are
completion-model measurements, not actual client/Kafka throughput. Record arrays,
payloads, client buffering and durable transactions are excluded.

Error-only completion removes the record-count-sized result buffer. Its constant
completion state is larger/slower in the one-record model (60% more bytes and
18.5% more time); two records have equal bytes and about 10.9% more time. Large
batches have much lower bytes and about 35–36% less bookkeeping time in this
model. Keep the tiny cases rather than inferring a universal speedup.

Tests exercise immediate and bounded concurrent out-of-order callbacks, first
error retention, cancellation, zero records, sync-only session compatibility,
and commit/abort only after all records complete. Real broker fault/correctness
and separate profile gates remain required before merging. No project-wide
performance target is claimed from this isolated buffer reduction.
