# Tumbling count after sharing hopping logic

Baseline: join source `9efa5833c87938f5dfbd8932cefeb881c4bb8829` (local
`66b5d4c3f6626eba7c289e45254bcc3cee2b48f9`). After: hopping implementation
in this change. Go 1.23.12, Linux/amd64, EPYC 9V74, one Go CPU. Both phases used:

```sh
go test ./stream -run '^$' -bench BenchmarkWindowExpiry -benchmem -benchtime=200ms -count=3 -cpu=1
```

| Existing tumbling case | Median before ns/op | Median after ns/op | After/before | allocs/op |
|---|---:|---:|---:|---:|
| keys=64/size=1024 | 1493 | 1489 | 0.997 | 22 → 22 |
| keys=64/size=1152921504606846976 | 1418 | 1442 | 1.017 | 22 → 22 |
| keys=4096/size=1024 | 1977 | 1977 | 1.000 | 22 → 22 |
| keys=4096/size=1152921504606846976 | 1603 | 1565 | 0.976 | 22 → 22 |

All independent checksum checks passed. Three short sequential repetitions give
regression screening, not a statistically established timing improvement.
Allocated bytes per record are stable except benchmark-run-length effects on
heap expiry growth. These are in-process Go bookkeeping tests with no Kafka
broker, network or durable backend. They do not qualify Kafka Streams speedup.

After source SHA256:
- `stream/window.go`: `7df5aca5d4e4a400eb3f3c38ad28374df444b746545c65723b7282e05852f195`
- `window/window.go`: `7d079c66e518400bc1d347c63e7480aeb595715a061ea923498ce30f04504e5d`
