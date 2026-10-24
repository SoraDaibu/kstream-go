# Grouped aggregation

GroupByKey declares grouping by the existing partitioned key. Count keeps its
specialized int64 implementation; Reduce preserves the value type; Aggregate
uses a package function to introduce a different result type in Go.

```go
grouped := events.GroupByKey()
counts := grouped.Count("counts")
latest := grouped.Reduce("latest", func(old, value string) (string, bool) {
    return value, false
})
lengths := stream.Aggregate(grouped, "lengths", serde.Int64,
    func() (int64, bool) { return 0, false },
    func(key, value string, old int64, oldNull bool) (int64, bool) {
        return old + int64(len(value)), false
    })
counts.ToStream().To("counts-out")
latest.ToStream().To("latest-out")
lengths.ToStream().To("lengths-out")
```

All callback bools mean null. Ordinary zero, empty string and empty byte values
are data. Aggregate calls its initializer when no aggregate exists, then passes
its value and null flag to the aggregator. Returning null deletes the aggregate
and emits a tombstone. The next accepted record initializes again. Reduce uses
the first value directly and calls its reducer only when state already exists.

Null input keys or values are skipped by both operations. Outputs carry the
maximum timestamp over the current aggregate lifetime, including a deletion
update. After deletion, a new lifetime starts at the next record's timestamp.
Every update is emitted; caching and suppression are not implemented. Callbacks
must be deterministic and must not mutate shared values outside task ownership.

The result is a KTable supporting ToStream and stream-triggered inner/left lookup
joins, including nullable results. State remains task-local, staged transactionally and restored from the
committed backend with its timestamp. This does not implement retraction-based
KTable aggregation. GroupByKey after a key-changing operation is rejected until
an explicit broker repartition boundary is applied; it cannot repair partition ownership by
merely changing an in-memory map key.
