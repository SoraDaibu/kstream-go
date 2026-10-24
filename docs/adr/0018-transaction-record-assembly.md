# ADR 0018: Owned transaction record arrays

Status: Proposed; local Go 1.27.3 allocation/correctness gates passed, broker
qualification pending. The design preceded implementation.

The isolated group-backend assembly benchmark allocates 4,146 objects for 4,096
output records, even with no transport or result allocation. Most output records
are individually allocated kgo.Record objects and the pointer slice repeatedly
grows. This is executable allocation evidence, not a claim about production CPU
dominance; whole-process broker profiling remains a separate diagnostic.

Go 1.27.3 independently reproduced 4,147 objects for 4,096 outputs before the
change. The original Go 1.23 sample above is historical, not a compiler
comparison. Retain both phases of the new-toolchain measurement separately.

Count output/state/checkpoint/barrier records per group transaction. Allocate
one exact record array and one exact pointer array; populate records in the same
order and pass pointers to the synchronous client API. Each call owns fresh
arrays. Do not pool, reuse or mutate them after dispatch: hooks or client results
may retain individual record pointers. Borrow key/value bytes only within the
existing transaction-buffer lifetime contract. No unsafe code or synchronization.

An individual retained pointer keeps the complete record-struct array alive.
This is a lifetime/memory tradeoff and a reason to avoid pooling and to measure
actual broker RSS rather than inferring it from allocation counts. It does not
change the borrowed lifetime of transaction payload bytes.

Check missing/duplicate records, null versus empty values and cross-call pointer
identity. Preserve every existing error/abort/checkpoint decision. Compare fixed
assembly workloads before/after and retain raw samples, then run real broker,
restart/rebalance/kill and independent DSL gates. Allocation reductions alone do
not establish the 2.5x target or improved Kafka end-to-end throughput.
