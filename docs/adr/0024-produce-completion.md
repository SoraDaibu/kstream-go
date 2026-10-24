# ADR 0024: Transaction produce completion without result retention

Status: accepted for evaluation; benchmark and broker validation required.

Kafka transaction adapters consume only the first production error; they do not
use ProduceSync's per-record result list. The retained broker profile attributes
roughly 54MB of allocation to ProduceSync in a 2M-output whole-process filter
workload. franz-go already uses one shared completion callback: do not assume
there is a per-record closure to remove. Target the result buffer itself.

Use the client's public Produce API with one task-transaction completion object,
one callback, and a wait group. Register the complete count before producing so
immediate callbacks work. Wait for every callback before deciding commit/abort,
even after an error or context cancellation. Record the first callback error;
protect rare concurrent error publication with sync.Once. Successful callbacks
only decrement the wait group, as in ProduceSync. Do not add operator goroutines,
pool records, recycle published arrays or change payload lifetimes.

Keep the existing session contract compatible. An optional internal Produce
capability selects this path for real franz-go clients/sessions; custom test or
injected sessions exposing only ProduceSync keep that fallback. An optional
producer must invoke its callback exactly once per submitted record, including
failure/cancellation. Real franz-go documents serial callbacks; the helper also
supports concurrent completions. No runtime reflection is needed.

All transaction validation, fencing, ownership checks, finalization and poison
rules remain in their existing owners. A produce error still aborts; commit is
never attempted until every callback completes. Fresh record structs remain
owned for the call and can be retained by client hooks after completion.

Test synchronous/concurrent/out-of-order callbacks, multiple errors, canceled
contexts, zero records, compatibility fallback and commit/abort ordering. Compare
completion bookkeeping against a result-retaining model with the same callback
and wait-group work, across tiny and large batches. Retain negative timings and
benchmark scope limits. Broker correctness/fault tests and separate profiles must
validate the real client path; removing a buffer alone does not prove throughput
improvement or meet the project-wide 2.5x goal.
