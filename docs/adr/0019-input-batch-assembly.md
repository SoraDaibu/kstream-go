# ADR 0019: Exact input batch segments

Status: Proposed; design preceded implementation. Local Go 1.27.3 allocation,
order/ownership and full validation gates passed; broker qualification pending.

The retained Go 1.27.3 whole-process filter profile attributes about 667MB of
sampled flat allocation to RunGroup. Source-line inspection identifies the
per-partition record slice append as the dominant allocation site. Repeated
geometric growth copies record headers and creates avoidable GC work.

Validate each polled record's partition against the immutable assignment and
count records per partition. Allocate one exact record-header array for the
bounded poll and give each task a full-slice-expression segment, with capacity
limited to its own count. Preserve first-seen partition order and per-partition
record order across topics. Keep payload bytes borrowed during synchronous
processing exactly as before. Build complete poll positions from the same rows.

Restore/validate each task once per prepared batch, then keep the existing
assignment-revision and abort/reset gates before processing. Any unowned input
rejects the whole preparation before task restoration. No pooling, retained
cross-poll buffers, additional goroutines/channels, or synchronization.

Compare grouping against the prior growing-slice algorithm on the same Go
1.27.3 compiler. Cover contiguous/interleaved partitions, multiple topics,
null/empty values, offsets and timestamps, capacity isolation and unowned input.
Broker restart/rebalance/kill gates and the full differential suite remain
required. Allocation/assembly timing alone is not end-to-end qualification.
