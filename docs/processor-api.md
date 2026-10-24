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
cross-type processor factories, timestamp extraction or interactive queries.
The rolling [Kafka ProcessingContext contract](https://kafka.apache.org/39/javadoc/org/apache/kafka/streams/processor/api/ProcessingContext.html)
guides the design; executable reference tests remain pinned to 3.9.1.
