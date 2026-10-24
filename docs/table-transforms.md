# Table transforms

A KTable represents current rows keyed by the existing partitioned key. Filter
and FilterNot create a materialized table with matching or nonmatching rows.
When a new update no longer matches, it deletes the derived row. Using
table.ToStream().Filter instead would merely drop that update and leave a
downstream materialized row unchanged.

```go
accounts := stream.Table(b, "accounts", "accounts-store", serde.String, serde.String)
active := accounts.Filter("active-store", func(key, value string) bool {
    return value != "inactive"
})
labels := stream.MapTableValues(active, "labels-store", serde.String,
    func(key, value string) (string, bool) {
        return key + ":" + value, false
    })
labels.ToStream().To("labels")
```

MapTableValues is a package function because Go methods cannot introduce a new
result type parameter. Its callback returns (value, null), so empty strings,
zero values and empty byte arrays are ordinary data. Callbacks receive a
read-only key and non-null value. Input tombstones bypass all predicates and
mappers and delete the derived row; they cannot be revived by a table mapper.
Null source keys are skipped. Updates keep the input key and timestamp.

Every result requires a unique store name and value codec. Its state participates
in the same task transaction as upstream source/aggregate state, outputs and
checkpoints. Codec/read/backend failures require a freshly restored task; a
partially staged derived table cannot be reused. Results support ToStream and
current-value inner/left lookup joins. These operations preserve partitioning.

There is no cache coalescing, suppression, versioned/as-of lookup or table
retraction aggregation here. Reference traces run with caching disabled; exact
delete/update emission is an independent oracle gate for each change.
