# Typed processor dispatch

Go 1.27.3, linux/amd64, EPYC 9V74, one Go benchmark CPU. Five sequential samples
per case, 500ms each. See [raw samples](samples.txt).

```sh
go test ./stream -run '^$' -bench '^BenchmarkTypedProcessorDispatch$' \
  -benchmem -benchtime=500ms -count=5 -cpu=1
```

| Inputs per transaction | Same-type method median ns/transaction | Cross-type function median ns/transaction | Bytes/transaction, both | Allocations/transaction, both |
|---|---:|---:|---:|---:|
| 1 | 335.5 | 334.1 | 320 | 3 |
| 256 | 17522 | 17764 | 320 | 3 |

Both factories enable metadata tracking. Scalar codecs decode without allocation;
the cross-type handler converts signed numeric records to unsigned numeric
records. A terminal observer checks every value through a checksum. Setup is
outside timing. Real task staging/checkpoints are included, but durable storage,
sink serialization, Kafka and payload allocation are excluded.

The unchanged allocation counts at 1 and 256 inputs show no per-record adapter
allocation in these cases. Timing differs by about -0.4%/+1.4%, within the spread
of these unisolated sequential samples; no throughput improvement is claimed.
This does not establish the cost of arbitrary user callbacks or feature parity.
