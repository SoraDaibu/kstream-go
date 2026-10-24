# ADR 0021: Explicit bounded output capacity

Status: Design before implementation; allocation, ownership and broker gated.

After exact input preparation, Task.Emit output-slice growth is the largest
remaining sampled Go allocation site (about 657MB of a 2102MB filter profile).
The transaction must retain every output until the backend decision. Removing
geometric record-header slice growth can reduce allocation and GC work without
changing callbacks, output order or the durability protocol.

Add execution.WithOutputCapacity(n), a bounded initial record-header capacity
hint. Default zero keeps ordinary append growth. Allocate only when the first
output is emitted; zero-output transactions allocate no output buffer. Each
transaction gets a fresh array, never a pool or reused committed buffer. Preserve
payload cloning, null/empty values and existing backend buffer lifetimes. A hint
is not an output count limit; fanout can grow beyond it.

Choose explicit configuration rather than learned cardinality: filter/fanout
callbacks can change emission rates between batches. A large hint wastes memory
for sparse output. Validate both dense and sparse/mismatched workloads and
document that tradeoff. Bound hints to 65,536 records; reject invalid options
before opening/fencing a backend. Do not change the default library behavior.

Measure Go 1.27.3 allocation/time with representative batch sizes and emission
rates, retain all regressions, then run the same verified broker experiment with
an explicit hint for its known one-output-per-input workloads. Record the hint
in the manifest. Independent semantics, transaction failure and retained-output
ownership gates remain mandatory; this is not production qualification.
