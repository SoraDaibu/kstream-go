# Source/sink compilation allocation guard

Go 1.27.3, linux/amd64, EPYC 9V74, one benchmark CPU. Before source is
`6278139e8577a7d23607a7a4b3d8c24562b6fa1e`. The refactor shares source decoding
and sink serialization between public topics and explicit broker exchanges.

```sh
go test ./stream -run '^$' \
  -bench 'BenchmarkWindowExpiry|BenchmarkTypedProcessorDispatch' \
  -benchmem -benchtime=200ms -count=1 -cpu=1
```

[Before](before.txt) and [after](after.txt) preserve all eight short cases.
Existing count cases retain 21 allocations per input; same/cross-type processor
cases retain 320 bytes and 3 allocations per transaction at batch sizes 1/256.
Expiry-heavy amortized bytes vary slightly with iteration count. These single
short samples are allocation checks and support no timing or Kafka speed claim.
They exclude actual exchanges and durable broker I/O. New exchange routing,
recovery and abort semantics require the dedicated broker/reference gates.
