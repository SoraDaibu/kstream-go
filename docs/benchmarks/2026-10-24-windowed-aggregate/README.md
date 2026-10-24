# Typed window-fold reduce regression check

Go 1.27.3, linux/amd64, EPYC 9V74, one benchmark CPU. Before source is
`15d2eb90ae6fbb9204708c6672a737c4d4c867ce` plus the identical new benchmark file.
The benchmark reduces numeric ones in one long-lived window and checks output
with an independent arithmetic checksum. It includes serialization/task state
but excludes durable storage, payload growth, broker I/O and window expiry.

```sh
go test ./stream -run '^$' -bench '^BenchmarkWindowReduce$' \
  -benchmem -benchtime=300ms -count=5 -cpu=1
```

| Keys | Before median ns/input | After median ns/input | Bytes/input, both | Allocations/input, both |
|---|---:|---:|---:|---:|
| 64 | 1611 | 1826 | 925 | 22 |
| 4096 | 1863 | 1901 | 959 | 22 |

The [sequential before](before.txt) and [after](after.txt) samples initially show
13.3%/2.0% higher times, which warranted a controlled alternating follow-up rather
than assuming that sharing code had no cost.

[Five alternating pairs](paired.txt) pin before/after to logical CPU 0, 1s per
sample, reversing order in alternating pairs. Median paired after/before times
are 1.011 for 64 keys (range 0.946–1.044) and 0.977 for 4096 keys (0.971–1.025).
Every sample preserves the same byte/allocation totals. The sequential slowdown
does not reproduce as a consistent large paired regression. Keep both sets:
timing remains noisy and neither a speedup nor exact zero overhead is established.

The typed shared loop supplies apply/result-codec functions, without interface
boxing of aggregate values or extra per-record allocations in these cases.
Counts retain their separately specialized loop; this check exercises reduce.
Broader window/state/broker performance qualification remains open.
