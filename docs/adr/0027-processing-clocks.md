# ADR 0027: Transactional clocks for connected processing stages

Status: accepted for implementation; independent Java and broker gates required.

The initial repartition runtime co-locates all stages for one partition number.
Exposing its partition-wide clock through every custom processor allowed an
upstream t=100 to advance a downstream stage that had consumed only t=9. Kafka
Streams' logical subtopologies require separate processing clocks. Clock ownership
must follow the logical graph without forcing its Java task or threading model.

Compute connected components at topology build, cutting only broker delivery
edges into repartition sources. Merge and table-lookup edges join components.
Derive stable IDs from length-prefixed sorted logical source-name bytes, encoded
as URL-safe base64 so checkpoint JSON cannot normalize distinct Go strings. Public same-type/cross-type
processors receive a scoped context that delegates stores and record metadata.
Source observers advance the component clock before decoding or filtering, with
no goroutines, channels or per-record interface conversions. The single-component
path retains the native task context and unchanged source dispatch.

Persist optional complete ScopeTimes maps alongside StreamTime in the execution
snapshot, streaming checkpoint and transaction contracts. Copy on first staged
advance; publish only after the durable decision and local state application.
Backends own their retained copies. Kafka checkpoints omit nil metadata to retain
ordinary wire bytes, and nil manual transactions preserve existing clocks.
Restore validates clock names and 0 <= clock <= native task time, before factories
can observe state. Callback/commit failure requires the existing poison/restore
protocol. Operator-local window clocks remain independent.

Adding a word to the transaction descriptor initially increased its allocation
size class by 16 bytes and produced a small timing regression. Reuse the private
descriptor inside the exclusively owned Task and clear it on every completion
path. Commit receives a value copy; all escaping slices, payloads and offset maps
keep fresh ownership, and scope maps use copy-on-write. No buffer pool or output
array reuse is introduced. Retained-transaction tests must pin this distinction.
Retain both initial and final paired measurements, including regressions.

No reliable per-stage clock can be inferred from an older shared-clock checkpoint.
Old checkpoints remain readable, but upgrades of multi-stage custom-processor
applications require a fresh application ID and replay. Stable source names and
topology remain deployment requirements; automatic migration is separate work.

Validate unrelated sources, merged/filtered prefixes, typed processors, exchange
delivery, replay, callback/commit rollback, map ownership, ordinary JSON bytes,
and both snapshot/streaming restore. Add two independent public Java clock traces
and verify restored downstream clocks against older inputs after a real Kafka
worker restarts with a fresh Pebble store. Benchmark ordinary dispatch/window
paths as well as the new scoped path. Cross-source timestamp scheduling, idle
controls, punctuation and heterogeneous stage scheduling remain open.
