# Initial scheduling evaluation — 2026-10-23

Source commit: `fd1761a61aec783393f06a6556f2088d9d434ee7`.
[Successful run](https://github.com/SoraDaibu/kstream-go/actions/runs/39997157363).
Go 1.23.12, Linux/amd64, GitHub-hosted AMD EPYC 9V74 runner with four logical CPUs.
The raw [environment](environment.txt), [one-CPU](scheduling-cpu1.txt) and
[four-CPU](scheduling-cpu4.txt) outputs are retained without editing.

Exact commands (from the repository root):

```sh
go version
lscpu
GOMAXPROCS=1 go test ./execution -run '^$' -bench BenchmarkSchedule -benchmem -benchtime=200ms -count 3
GOMAXPROCS=4 go test ./execution -run '^$' -bench BenchmarkSchedule -benchmem -benchtime=200ms -count 3
```

Selected medians over three samples, GOMAXPROCS=4. Times are microseconds per
**entire batch**, not per record. Bounded mode has four workers; strategy=1
uses one goroutine per task (its workers parameter is ignored).

| Tasks | Records/task | CPU steps/record | Bounded, us/batch | Per-task, us/batch |
|---|---|---|---|---|
| 8 | 1 | 0 | 7.365 | 11.230 |
| 256 | 1 | 0 | 158.811 | 188.999 |
| 8 | 256 | 128 | 483.257 | 411.682 |
| 256 | 256 | 128 | 13,634.300 | 11,749.386 |

Bounded workers were faster for these small batches and used fewer allocations.
Goroutine-per-task was faster for the larger batches on this runner. For example,
the 256-task/one-record/zero-work case allocated 796 objects per batch with four
workers versus 1,302 in per-task mode. This does not establish one strategy as a
universal throughput winner.

Retain bounded workers as the conservative initial default for explicit active
concurrency limits, and retain per-task mode for comparison. The production
choice remains provisional: these short microbenchmarks use task-private no-op
backends and omit broker I/O, persistent stores, live-task fairness, queue
backpressure, resident memory and tail latency. Repeat longer runs on a stable
host and representative workloads before making a production scheduling choice.
