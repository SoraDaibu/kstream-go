# Ordered branch routing overhead

Go 1.27.3, linux/amd64, EPYC 9V74, one Go benchmark CPU, three sequential 200ms
samples per case. [Raw samples](samples.txt) retain every result.

```sh
go test ./stream -run '^$' -bench '^BenchmarkBranchRouting$' \
  -benchmem -benchtime=200ms -count=3 -cpu=1
```

| Predicates | First match median ns/batch | Last match median ns/batch | No match median ns/batch |
|---|---:|---:|---:|
| 1 | 18063 | 18106 | 16110 |
| 8 | 16729 | 20309 | 20043 |
| 64 | 17035 | 54301 | 51235 |

Each batch has 256 inputs. All cases allocate 320 bytes and 3 objects per
transaction, with no per-record router allocation. Scalar codecs, task staging
and checkpoints are included; durable storage, payloads, sink serialization and
Kafka are excluded. An output checksum detects missed/duplicated delivery.

Early-match cost remains similar as predicate count grows; late/no-match cost
scales with predicates evaluated. These short unisolated samples do not claim
Kafka performance improvement or a precise slope. No router metadata is added
to records, and callbacks run synchronously under the existing task owner.
