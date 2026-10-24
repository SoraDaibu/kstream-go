# ADR 0014: Observe committed batch shape before tuning scheduling

Status: Accepted instrumentation; observations do not justify coalescing.

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

The first instrumented run measured median means of 3774 filter and 3922 count
records per batch, against a 4096 cap. Most batches were already full. Do not
introduce a delay on the assumption of tiny batches. Early filter phases also
showed larger processing times than later phases; warmup/system effects remain
a hypothesis. Increase CI warmup and measurement to one million records each
while preserving the original script defaults for reproduction, and keep both
sets of raw results distinct. This changes the measurement protocol, not runtime
polling behavior.
