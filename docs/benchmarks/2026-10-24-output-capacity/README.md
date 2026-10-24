# Explicit output capacity on Go 1.27.3

Go 1.27.3, Linux/amd64, EPYC 9V74, one Go CPU. All capacity choices run in the
same executable. The benchmark includes input setup, raw task processing,
payload cloning and independent enumeration of every expected output sequence.
It excludes Kafka, transport and durable state. Each callback and committed
input position is checked; fanout, sparse and changing output rates are explicit.

```sh
go test ./execution -run '^$' -bench '^BenchmarkOutputCapacity$' -benchmem -benchtime=200ms -count=3 -cpu=1
```

| Inputs/batch | Shape | Initial capacity | Median ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|---:|
| 256 | dense | 0 | 34026 | 49840 | 524 |
| 256 | dense | 256 | 29358 | 25408 | 516 |
| 256 | sparse | 0 | 15967 | 952 | 14 |
| 256 | sparse | 256 | 20106 | 24904 | 12 |
| 256 | sparse | 4 | 15921 | 680 | 12 |
| 256 | none | 0 | 15721 | 320 | 3 |
| 256 | none | 256 | 16063 | 320 | 3 |
| 256 | fanout | 0 | 81632 | 181936 | 1550 |
| 256 | fanout | 256 | 71960 | 157504 | 1542 |
| 256 | fanout | 768 | 58505 | 75584 | 1540 |
| 256 | bursty | 0 | 28905 | 25396 | 269 |
| 256 | bursty | 256 | 26380 | 25156 | 264 |
| 4096 | dense | 0 | 826937 | 1458357 | 8211 |
| 4096 | dense | 4096 | 531715 | 368960 | 8196 |
| 4096 | sparse | 0 | 260116 | 12592 | 138 |
| 4096 | sparse | 4096 | 339014 | 360896 | 132 |
| 4096 | sparse | 64 | 259523 | 6592 | 132 |
| 4096 | none | 0 | 230826 | 320 | 3 |
| 4096 | none | 4096 | 247449 | 320 | 3 |
| 4096 | fanout | 0 | 2029033 | 4743346 | 24599 |
| 4096 | fanout | 4096 | 2441633 | 4546898 | 24585 |
| 4096 | fanout | 12288 | 1032084 | 1106240 | 24580 |
| 4096 | bursty | 0 | 562392 | 737183 | 4184 |
| 4096 | bursty | 4096 | 444254 | 364928 | 4164 |

At 4,096 dense outputs, a matching hint uses 74.7% fewer allocated bytes and
about 35.7% less median time in this local task workload. Matching a three-output
fanout uses 76.7% fewer bytes and about 49.1% less time. Clone allocations remain;
this primarily removes repeated record-header array growth.

The negative cases matter: a 4,096 hint for only 64 outputs allocates about
28.7x as many bytes as default growth and is about 30.3% slower. Reserving only
4,096 for 12,288 outputs also has a slower median in these samples. A correctly
sized 64-output hint uses fewer bytes, and zero-output transactions have identical
320B/3 allocation counts regardless of hint. This is explicit configuration,
not an automatic default optimization or an output limit.

Three short sequential samples are regression screening, not a significance
claim. Preserve timing variation and all mismatched cases. Default before/after
TransactionBatch files use baseline source `6eddd0c8b8fc2bdfb660a48b5d4c185d9fa9d136`
(local `0dd95aa`) with the same Go 1.27.3 compiler:

```sh
go test ./execution -run '^$' -bench BenchmarkTransactionBatch -benchmem -benchtime=300ms -count=3 -cpu=1
```

All eight default allocation counts remain unchanged. The full independent
oracle runs both capacity 0 and 256; broker runs declare their actual hint.
No local result here establishes Kafka Streams throughput or production parity.

After source SHA256:
- `execution/task.go`: `742e753e986dd633867b5af08d131e312397b6e667fa8212762a616c780c9b0d`
- `execution/options.go`: `586076c32c1af42bd7d2464d6f2192fa3d0dc0c19c847796b578011f62045904`
- `execution/output_capacity_bench_test.go`: `a21afc01c38127936e15280ebfca3796153fa300cf4d1ce9a9f8943f9c2a686a`
