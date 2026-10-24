# Explicit broker repartitioning

Repartition creates an internal topic sink and a separately consumed source.
It does not call downstream operators directly. This lets a key-changing stream
feed key-local state after Kafka routes records by their encoded new keys.

```go
changed := stream.SelectKey(events, "user-key", serde.String,
    func(r record.Record[string, string]) (string, bool) { return r.Value, r.NullValue })
changed.Repartition("by-user").GroupByKey().Count("counts").ToStream().To("counts-out")
plan, err := builder.Build()
// Handle err before provisioning or starting workers.
inputs, err := plan.InputTopics(applicationID)
exchanges, err := plan.RepartitionTopics(applicationID)
config.Topics = inputs
config.Kafka.RepartitionTopics = exchanges
```

Handle each setup error. Provision all inputs and exchanges with equal stable
partition counts for the current group worker. Exchange names are
`applicationID-name-repartition`; use stable application IDs, logical names and
codecs across restarts. Generated Kafka names must use valid characters and fit
249 bytes. Resolved source collisions and user sinks targeting an exchange are
rejected before task open. Returned topic lists are owned copies.

```mermaid
sequenceDiagram
    participant Up as Upstream task and adapter
    participant Kafka as Kafka broker
    participant Down as Destination task and adapter
    Up->>Up: Change key; stage local state and exchange records
    Up->>Up: Producer selects partition from encoded key
    Up->>Kafka: Commit state + exchange output + original input offsets
    Down->>Kafka: Poll read-committed exchange input
    Kafka-->>Down: Committed records with broker partition and offset
    Down->>Down: Apply key-local operators
    Down->>Kafka: Commit state + user output + exchange input offsets
```

The Kafka adapter forces standard keyed hashing for configured exchanges, even
with a custom ordinary OutputPartitioner. Changelogs keep explicit task partition
routing. Null keys receive a valid producer partition; null/empty payloads and
timestamps pass through the exchange. Task.Emit's output partition field is not
a substitute for the adapter's key routing. A model transport must assign the
actual destination partition and consumed offset before delivering a record.

Exchange topics require cleanup.policy=delete without compaction. Startup checks
that policy through DescribeConfigs, so exchange users need that topic permission
in addition to normal read/write/group permissions. Compaction could remove
intermediate updates before consumption and is rejected. Retention must cover
the application's backlog/recovery needs. Topic creation and ongoing retention
administration are explicit deployment responsibilities in this version.

Upstream and downstream state, output and positions use the existing transaction
protocol. Downstream consumers see committed exchange records; aborted records
must not affect state. Restart restores committed task-local state and consumed
positions. There is no in-process exchange bypass or asynchronous operator queue.

The initial worker coalesces compiled stages into one task per partition number
and requires equal-count eager co-location. Kafka still separates stage delivery
and transactions. Independent stage scheduling, heterogeneous counts, automatic
topic provisioning and custom internal partitioners remain open.

Context.StreamTime currently describes that coalesced partition task, including
its other stages. It differs from Kafka's separate subtopology task clocks: after
upstream t=100, a delayed exchange t=9 can observe task time 100. Window operators
retain their own input clocks and still accept eligible t=9 records. This boundary
is explicitly [tested](../stream/repartition_test.go); subtopology-scoped custom
processor clocks and cross-source timestamp scheduling are not yet equivalent.
No global order across independent stages/topics is promised.

Backends must explicitly declare execution.RepartitionValidator support for
exchange topologies; backend wrappers must forward it. Unknown transports fail
before task open. The memory model declares caller-driven delivery only.
