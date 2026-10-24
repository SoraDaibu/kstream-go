# ADR 0014: Observe committed batch shape before tuning scheduling

Status: Instrumentation decision before measurement; coalescing remains gated.

The first broker run measured a 2.06x median paired filter ratio and 6.90x count
ratio. A repeat used a different hosted CPU and produced different timings.
Filter also spent proportionally more elapsed time outside application CPU than
count. Small polling/transaction batches are a hypothesis, not an established
cause; hardware and broker contention must remain visible.

Expose an optional OnBatch observer on the Kafka group worker, invoked only after
the durable decision and local applies succeed. Report input/task counts and
processing duration. Keep the callback under the existing worker owner and sample
no clock when it is nil. It creates no operator goroutine/channel and cannot
change a transaction that already committed. Observers must return promptly.

The benchmark accumulates phase-specific counts and size histograms and logs one
summary during shutdown. Verify observed records against the independent driver.
Record the resulting processing batch count and mean size alongside throughput,
latency, RSS and CPU. Measure before introducing any bounded poll coalescing or
changing defaults. Any future linger must preserve the complete poll prefix,
assignment changes, abort/replay behavior and caller memory/time bounds.
