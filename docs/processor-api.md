# Typed processors and source metadata

KStream.Process creates one synchronous handler per task from a factory.
Its Context owns staged stores and exposes the task's stream-time. Declare
whether the processor changes keys so downstream grouping/join validation can
enforce partition ownership. Forward only while the handler is running.

```go
events.Process("annotate", false,
    func(ctx processor.Context, next processor.Forward[string, string]) processor.Handler[string, string] {
        return func(r record.Record[string, string]) error {
            source, present := ctx.RecordMetadata()
            if present {
                r.Value = fmt.Sprintf("%s:%d:%d", source.Topic, source.Partition, source.Offset)
                r.NullValue = false
            }
            return next(r)
        }
    }).To("annotated")
```

For distinct input/output types, use the package-level stream.Process function
and a processor.Supplier. Go methods cannot declare the extra type parameters.
The original method and Factory[K,V] remain available for same-type processors.

```go
lengths := stream.Process(events, "lengths", serde.String, serde.Int64,
    func(ctx processor.Context, next processor.Forward[string, int64]) processor.Handler[string, string] {
        return func(r record.Record[string, string]) error {
            return next(record.Record[string, int64]{
                Key: r.Key, Value: int64(len(r.Value)), Timestamp: r.Timestamp,
                NullKey: r.NullKey, NullValue: r.NullValue,
            })
        }
    })
lengths.To("lengths-out")
```

Suppliers own task-local handlers and may use staged stores through Context.
They may forward zero or more records, choose timestamps, and change null flags.
Always return forwarding errors. A callback failure aborts all staged output,
state and input offsets and poisons the task until it is restored.

The package function always invalidates key partitioning, including the example
above, which copies keys. Downstream key-local grouping/joins reject it until
an actual repartition exchange is implemented. For a value-only transform with
preserved partitioning, use MapValues. No fixed-key custom Processor API exists
yet. Output codecs are explicit; numeric zero is data, and NullValue=true is a
tombstone independent of the Go value field.

RecordMetadata identifies the original consumed input. Synchronous downstream
processors and flat-map outputs see the same source topic, partition and offset.
Returning (metadata, false) means no source record is currently being processed,
including factory initialization, backend commits and after processing ends.
Check the presence flag. A copied record.Metadata value may be retained; the
Context belongs to the task owner and must not be retained for concurrent use.

Custom DSL Process factories enable tracking automatically. Lower-level callers
using execution.New and Task.Register opt in with execution.WithRecordMetadata().
Without that option their RecordMetadata calls return present=false. Topologies
containing only built-in operators avoid unnecessary metadata tracking.

Replayed offsets do not invoke handlers. Metadata is cleared after normal
processing, errors, panics and owner exit. It adds no per-record metadata object
or field to every typed record. Store writes, emissions and offsets participate
in task transactions; external effects of user callbacks do not roll back.

The experimental Context interface gains RecordMetadata; custom implementations
and test doubles must implement it. This does not add headers, punctuation,
timestamp extraction or interactive queries.
The rolling [Kafka ProcessingContext contract](https://kafka.apache.org/39/javadoc/org/apache/kafka/streams/processor/api/ProcessingContext.html)
guides the design; executable reference tests remain pinned to 3.9.1.
