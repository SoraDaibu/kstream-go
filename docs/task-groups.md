# Atomic task groups

Use a transaction group when one durable input prefix contains work for several
partition tasks. It stages every participant before making one backend decision.
The memory backend models this capability; a Kafka consumer-group adapter is
separate work and remains required for group-managed EOS v2.

```go
backend := memory.New()
group, err := execution.NewGroup(backend)
if err != nil { return err }
left, err := topology.NewTask(ctx, leftSpec, group)
if err != nil { return err }
defer left.Close()
right, err := topology.NewTask(ctx, rightSpec, group)
if err != nil { return err }
defer right.Close()
return group.Process(ctx, []execution.TaskBatch{
    {Task: left, Records: leftRecords},
    {Task: right, Records: rightRecords},
})
```

The backend must implement AtomicBackend. Tasks use the same group at
construction and processing; each task appears once in a call. Group-owned tasks
reject independent Task.Process/ProcessBatch before callbacks. Input slices and
the number of participants are explicitly bounded by the caller. Each group has
one processing owner; no new goroutines/channels are created for operators.

Routing and ownership are checked before callbacks. A callback/store/commit
failure poisons every participant, including failures with ambiguous durable
outcomes. Restore fresh task incarnations before resuming. A local-store failure
after the backend commits also poisons every participant: the group cannot roll
back that durable decision. All local applies must succeed before the runtime
publishes task offsets and clocks. External callback effects remain outside the
transaction contract.

An empty set does nothing. A nonempty input set containing only checkpointed
records still invokes CommitTasks with an empty state transaction set. A future
Kafka group adapter needs that opportunity to advance a replayed poll's group
offsets. It must verify coverage of every polled record before committing; the
generic memory model does not know Kafka consumer positions or group metadata.

See [ADR 0011](adr/0011-atomic-task-groups.md). Atomic model tests and the Java
operator oracle do not establish Kafka group assignment, rebalance safety or
production exactly-once guarantees.
