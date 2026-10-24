# ADR 0020: Processor source-record metadata

Status: Design before implementation; independent oracle and overhead gated.

Add record.Metadata{Topic, Partition, Offset} and Context.RecordMetadata returning
(metadata, present). It identifies the original consumed record during synchronous
record processing, including typed downstream forwarding and flat-map fanout.
It is absent during factory initialization, after callbacks/transactions and
during backend commits. Replayed source records do not invoke callbacks.

Track scalar topic/offset fields in the task and a callback-presence flag. The
partition comes from the validated task owner. Update the topic string only
when it changes, avoiding repeated heap-pointer writes within a source batch. Avoid
allocating metadata objects or copying source metadata through every typed
record. Clear presence after each callback and all fields on success/error/
panic/Goexit cleanup. Preserve single ownership and add no synchronization.

The first design retained the active input slice with an integer index. Although
batch allocations stayed unchanged, it made the single-record Process wrapper's
temporary input array escape: +1 allocation and 96 bytes per input. Reject that
design. Scalar metadata requires a source-topic pointer write per callback but
does not retain the input array or payload, preserving stack allocation in the
single-record path. Recheck both single-record and batch benchmarks.

Always-on scalar tracking still adds work to built-in-only topologies. Instead,
the DSL compiler enables metadata when a public custom Process factory is added;
built-in implementation helpers do not enable it. Lower-level execution.New
callers opt in with WithRecordMetadata. Without it the accessor is absent, a
documented runtime configuration boundary; DSL custom processors need no option.
Only enabled tasks wrap source handlers during registration. Default handlers
and their record-processing loop remain unchanged; no metadata capability branch
runs per default record. Wrappers are setup allocations, not per-record objects.

Metadata copies may be retained as ordinary immutable values. Contexts remain
callback-scoped and cannot be called concurrently or retained for later work.
Headers, topic extraction, timestamp extractors and punctuation are separate
feature gates. This experimental Go Context interface gains a method; external
test doubles implementing it must supply the new capability explicitly.

Validate source topic/partition/offset with Kafka Streams 3.9.1's public Processor
API and TopologyTestDriver, both stores, three batch sizes and transaction modes.
Test multiple topics, fanout, replay and cleanup after normal and abnormal exit.
Benchmark unchanged task-batch workloads on Go 1.27.3 and retain overhead/allocs.
