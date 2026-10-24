# Stream-table joins

Join reads the current value of a task-local KTable when a stream record arrives.
Table updates alone do not emit joined records. Both inputs must use the same
builder and partitioned keys; joins after a key-changing operation fail Build
until a real repartition exchange is available.

```go
users := stream.Table(b, "users", "users-store", serde.String, serde.String)
events := stream.Stream(b, "events", serde.String, serde.String)
stream.LeftJoin(events, users, "enrich", serde.String,
    func(event, user string, rightNull bool) (string, bool) {
        if rightNull {
            return event + ":unknown", false
        }
        return event + ":" + user, false
    }).To("enriched")
```

Join is the non-null-result inner-join convenience. JoinNullable accepts a
joiner returning (value, null). LeftJoin additionally supplies rightNull so a
missing table value is distinguishable from a present empty string, zero or
empty byte value. All callback bools mean null; returning null emits a Kafka
null value with the stream record's key and timestamp.

Inner joins skip null stream keys, null stream values and missing table rows.
Left joins skip null stream values, but preserve null stream keys and pass a
null right value without looking up the store. A table tombstone deletes the
lookup entry. Results use the stream timestamp, even if the stored table value
has a newer timestamp. This is current-value lookup, not a versioned/as-of join.

Table state and source checkpoints participate in the same task transaction.
Failed processing requires restoration before reuse; replayed table and stream
records are skipped. The deterministic oracle supplies explicit cross-topic
arrival order. Kafka worker cross-input timestamp scheduling and co-partitioning
validation remain separate qualification gates.
