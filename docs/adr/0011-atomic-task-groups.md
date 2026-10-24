# ADR 0011: Atomic multi-task transaction boundary

Status: Proposed before implementation; model, oracle and broker integration gated.

The current task commits one partition's state/output/checkpoint independently.
A group consumer poll may contain several partitions. Committing that poll's
group offsets after only the first task's work would lose the other tasks' input.
Returning success from a deferred backend commit would also expose local state
before durability. Both approaches violate the existing task contract.

Add an execution.Group that owns a single bounded worker's transaction boundary.
It wraps an AtomicBackend capability that atomically commits an ordered set of
task transactions, validating every task's generation before publishing any.
Create topology tasks with the group as their backend and process them through
Group.Process. Ordinary Task.Process is rejected for these tasks so it cannot
accidentally commit a partial poll. Polling and group execution have one owner;
there are no new per-operator goroutines or channels.

Acquire every participant's ownership guard and validate all routing before any
callback. Stage each task synchronously, then make one durable backend decision.
Only afterward apply local mutation batches. Publish task offsets/clocks only
when every local apply succeeds. A callback, store, ambiguous commit or local
apply error poisons all participants; discard their derived processor state and
restore all participants from the committed backend. Panic cleanup releases
ownership and preserves the original panic. A busy task or pre-callback routing
error releases acquired guards without poisoning otherwise healthy tasks.

The group checks membership by its concrete backend identity and rejects duplicate
tasks. A group has one active processing call. Empty input sets do nothing; a
nonempty poll whose records are all already checkpointed still calls CommitTasks
with an empty state transaction set, allowing a future group adapter to advance
its polled offset prefix safely.

The reference memory backend models atomic validation and publication under one
infrastructure lock. This is preparation for a Kafka consumer-group adapter using
franz-go's public GroupTransactSession API and current group metadata. It does not
itself implement group assignment or EOS v2. Those require stable-offset fetches,
co-partitioned assignment, revocation/loss handling, offset/state/output atomicity
and kill/rebalance broker tests before any compatibility claim.
A Kafka group adapter must also verify that the participant checkpoints cover
every record in the poll before committing its group offsets; callers cannot
silently omit a polled partition from the durable decision.

Keep the existing single-task path semantically unchanged and reuse its staging
logic. Run the independent Java oracle after refactoring, test stale ownership
in any participant before all publication, and inject failures before/after the
durable decision and during local application. Benchmark the refactor's hot path;
do not infer Kafka Streams throughput from the model.
