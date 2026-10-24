# Go 1.27.3 finite-batch scheduling measurements

Source: 156a4db82703f199d7e4009fcf715c034697119c (PR31 source827c9cf0441d2b1f5561fb3212b735fd0d8f87d0).

Five 200ms samples per case, pinned CPU affinity [0] or [0,1,2]. No broker, durable commit, live fairness or tail-latency qualification. Each lane commits one record at a time; 256 records means 256 independent transactions, not one transaction. Order is sequential by benchmark case, not randomized/alternating.

| Go CPUs | Tasks | Records per lane | CPU work | Workers | Time bounded / per-task |
|---|---|---|---|---|---|
| 1 | 8 | 1 | 0 | 1 | 0.574 |
| 1 | 8 | 1 | 128 | 1 | 0.629 |
| 1 | 8 | 256 | 0 | 1 | 0.970 |
| 1 | 8 | 256 | 128 | 1 | 1.064 |
| 1 | 256 | 1 | 0 | 1 | 0.449 |
| 1 | 256 | 1 | 128 | 1 | 0.497 |
| 1 | 256 | 256 | 0 | 1 | 1.060 |
| 1 | 256 | 256 | 128 | 1 | 0.975 |
| 3 | 8 | 1 | 0 | 1 | 0.538 |
| 3 | 8 | 1 | 0 | 3 | 0.640 |
| 3 | 8 | 1 | 128 | 1 | 0.611 |
| 3 | 8 | 1 | 128 | 3 | 0.608 |
| 3 | 8 | 256 | 0 | 1 | 1.317 |
| 3 | 8 | 256 | 0 | 3 | 0.961 |
| 3 | 8 | 256 | 128 | 1 | 1.729 |
| 3 | 8 | 256 | 128 | 3 | 1.280 |
| 3 | 256 | 1 | 0 | 1 | 0.617 |
| 3 | 256 | 1 | 0 | 3 | 0.655 |
| 3 | 256 | 1 | 128 | 1 | 0.606 |
| 3 | 256 | 1 | 128 | 3 | 0.642 |
| 3 | 256 | 256 | 0 | 1 | 1.492 |
| 3 | 256 | 256 | 0 | 3 | 1.291 |
| 3 | 256 | 256 | 128 | 1 | 1.496 |
| 3 | 256 | 256 | 128 | 3 | 0.870 |

Ratios below one favor bounded workers. Bounded workers reduce overhead for one-record lanes on this runner. Larger lanes have mixed results, including cases that favor one goroutine per task. Retain the bounded default for its concurrency limit; do not infer a production winner from this finite no-op-backend benchmark.

Commands: GOMAXPROCS=N taskset -c CPU_LIST go test ./execution -run ^$ -bench ^BenchmarkSchedule$ -benchmem -benchtime=200ms -count=5

The code exposes no operator goroutine/channel. GoroutinePerTask remains an opt-in finite scheduler comparison. Current broker group workers run bounded serial task execution and do not use this finite scheduler.
