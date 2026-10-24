# Window lifecycle refactor: count regression gate

Go 1.27.3, linux/amd64, EPYC 9V74, one Go benchmark CPU. Before source is
`b3c50210e22ad24028b01b7bba85839d37409a67`. The final shared lifecycle supports
windowed reduce and caches each operator's committed clock per task incarnation.
The existing count payload/state format and arithmetic membership stay unchanged.

```sh
go test ./stream -run '^$' -bench '^BenchmarkWindowExpiry$' \
  -benchmem -benchtime=300ms -count=5 -cpu=1
```

| Keys | Window size | Before median ns/input | Final median ns/input | Before/final bytes | Before/final allocations |
|---|---:|---:|---:|---:|---:|
| 64 | 1024 | 1607 | 1635 | 896 / 880 | 22 / 21 |
| 64 | 2^60 | 1540 | 1516 | 880 / 864 | 22 / 21 |
| 4096 | 1024 | 2075 | 2001 | 1261 / 1257 | 22 / 21 |
| 4096 | 2^60 | 1773 | 1703 | 911 / 907 | 22 / 21 |

[Before](before.txt) and [final](after.txt) retain all five samples per case.
Expiry-heavy bytes vary slightly with the benchmark's iteration count and key
retention. Removing the repeated clock read removes one allocation per input in
all four cases. The 64-key expiry timing rises 1.7% in the sequential sample set;
the other three fall 1.6–4.0%. These are short unisolated measurements.

The first shared-lifecycle candidate retained a store read/clock decode per
input. Its [sequential samples](shared-candidate.txt) and
[five CPU-pinned alternating pairs](shared-candidate-paired.txt) are retained;
the paired 64-key expiry median after/before time was 1.044 (range 0.958–1.051).
That timing regression prompted the clock-cache revision. Clock state still
stages atomically, loads from the committed store on recovery and discards any
uncommitted cached value with a poisoned task.

[Final paired measurements](paired.txt) alternate before/after order on logical
CPU 0, five pairs, 1s per sample, for keys=64/size=1024. The median paired time
ratio is 0.959, range 0.909–1.037. Every final sample has 880 bytes/21 allocations
versus 896/22 before. The overlapping ranges and sequential timing difference
limit timing precision; this establishes an allocation reduction, not a Kafka
throughput improvement. The benchmark checks its independently modeled output
count and includes serialization/task transactions but no durable broker commit.
