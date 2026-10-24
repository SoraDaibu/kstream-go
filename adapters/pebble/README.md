# Shared persistent state

The adapter uses the public API of [Pebble](https://github.com/cockroachdb/pebble),
an existing Go storage engine, for WAL, atomic batches and physical compaction.
The task runtime owns logical namespaces and transaction ordering. It does not
reimplement a storage engine or expose engine details in the DSL. This follows
Kafka Streams' task-local state concept while sharing physical infrastructure
between tasks, as decided in [ADR 0007](../../docs/adr/0007-persistent-state.md).

```go
engine, err := pebble.Open("./state", pebble.Config{SyncWrites: true})
if err != nil { return err }
defer engine.Close()

task, err := topology.NewTask(ctx, spec, backend,
    execution.WithStoreFactory(engine.Task))
if err != nil { return err }
defer task.Close() // releases the namespace before the engine closes
return task.ProcessBatch(ctx, records)
```

Import `github.com/SoraDaibu/kstream-go/adapters/pebble` and `execution`.
Application ID, task ID and partition determine a stable namespace. A second
owner is rejected. Tasks share an engine's caches and background workers; they
keep exclusive ownership of their mutable state. Direct Scope users also must
serialize access to each scope and close scopes before Engine.Close.

SyncWrites selects synchronous WAL-backed local write batches. The default false
is a rebuildable persistent cache and makes no power-loss durability promise.
When paired with a Kafka backend, the backend transaction commits first; local
cache failure then requires restoration instead of retrying an input in place.
Store reads and scans return owned buffers and preserve empty versus null values.

Restoration conservatively resets the namespace. Backends with the optional
StreamingRestorer capability (Kafka and the reference memory backend) replay
ordered bounded mutation batches directly into the store. Legacy backends still
materialize a complete snapshot first. The cache is not reused by offset
checkpoint. Window operators also retain an O(N) derived expiry heap, so this
does not qualify larger-than-RAM window processing. Window/session/versioned
store APIs, efficient range scans and retention remain separate work. This is an
experimental adapter, not a complete Kafka Streams state subsystem.
