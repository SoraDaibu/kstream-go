# ADR 0022: Typed processor input and output boundaries

Status: accepted; 600 independent reference comparisons passed at source
`bf1c22cf6dde49e26eb1ce026b07f2ffcb0719ec`.

Kafka Streams' public Processor API permits distinct input and output types.
The existing Go KStream.Process method deliberately uses one key/value pair:
Go methods cannot introduce additional type parameters. Requiring interface{}
records or reflection to cross this boundary would put conversions in the hot
path and weaken compile-time checks.

Add processor.Supplier[KIn,VIn,KOut,VOut] and a package-level stream.Process
function. A supplier receives a task Context and typed output Forward, then
returns a typed input Handler. It runs once per compiled task, including graphs
shared by merged sources. Retain Factory[K,V] and the existing method without
changes. New output codecs are explicit at the DSL boundary.

The package function conservatively invalidates key partitioning, even when the
output key happens to equal the input key. Downstream key-local operations must
reject it until an explicit broker repartition boundary is applied (ADR 0026). This matches the intent of
Kafka Streams process while avoiding a false local routing guarantee. A future
fixed-key API needs a separate record/forward contract that enforces key identity.

Compile the supplier into the existing synchronous typed graph. No new runtime
operator, goroutine, channel, reflection or per-record interface conversion is
needed. Metadata tracking is enabled just as for the existing custom Process
method. Store writes, outputs and offsets retain the task's transaction and
poison/recovery rules. User code owns timestamp/null transformations and must
propagate downstream errors. Forward and Context are serial callback-only APIs;
retaining forward for later execution is unsupported.

Validate different key/value types, nulls and empty values, fanout timestamps,
source metadata, task-local state/recovery, shared factory ownership, partition
validation, and rollback on downstream/supplier errors. Add independent Java
ContextualProcessor traces with numeric intermediate key/value types and compare
all existing transaction/store configurations. Benchmark typed dispatch with
allocation-free codecs to expose any per-record adapter allocation; this is a
local overhead check, not Kafka throughput qualification.
