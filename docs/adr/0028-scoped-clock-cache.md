# ADR 0028: Cache scoped clocks within the current transaction

Status: rejected for this revision. The original runtime remains in place.
The final candidate improves scoped dispatch but fails an ordinary-control gate.

The connected-stage clock implementation reads its checkpoint map for every
clock observation and processor clock read, and writes the map on each advance.
The durable map is necessary at transaction boundaries; these repeated hot-path
lookups are not. The existing bounded task owner can maintain a scalar view.

Give each task one private clock state per logical scope; copied public context
handles share that state. Embed the handle with its private state in one scope
allocation so copy safety does not add another allocation per scope. Move the context registry
from graph compilation into execution, where both source observations and custom
processors obtain the same scope view. Cache the observed maximum for the current
transaction. An epoch invalidates the scalar at each new transaction; a wrap
invalidates all cached epochs. Setup and reads outside a transaction continue to
use committed metadata.

Link each advancing context once into a task-owned dirty list. After all input
callbacks succeed, copy the committed map once and write each advancing scope's
final timestamp. This bounds map work by advancing scopes per transaction rather
than accepted records. The list allocates no per-record or per-transaction nodes.
Clear its links and flags on every completion path. Callback/commit/panic/owner
exit failures retain the existing poison/restore protocol; cached staged values
cannot become committed progress. Group transactions still receive complete
clock snapshots before their atomic backend decision.

No public API, checkpoint encoding, operator ordering, worker count or state
ownership changes. No locks, channels, goroutines, reflection or unsafe code are
added. An optional private cache contains the registry, epoch and dirty list; ordinary
tasks keep their original size and field offsets. Larger scope objects cost
bounded setup memory proportional to scope count; measure this separately rather than hiding it outside the timed
dispatch benchmark.

Before acceptance, require at least 15% lower median time in scoped dispatch with
256-record batches, unchanged dispatch bytes/allocations per transaction, no
ordinary dispatch/window/pipeline median slowdown above 5%, and no setup median
time or allocated-byte increase above 15%. Use five alternating pinned Go 1.27.3
pairs and retain all cases. Require race, layers, vet, differential, live Java
clock and Go broker recovery gates. Shared scope views, epoch wrap, rollback and
retained checkpoint maps need direct regression coverage.

The first candidate explored tiny key/value clone coallocation instead. It
removed an allocation but prevented inlining and slowed the target cases; a
helper revision still failed screening. Reject both and keep the original clone
implementation. Reduced allocation counts alone are not an acceptance criterion.

Earlier cache layouts failed the 5% ordinary-control timing gate, and review
found that storing mutable cache fields directly in the public context made
copied handles stale. Retain those measurements, preserve the ordinary Task
layout, and explicitly test reads and observations through copied handles.

The final private-cache series records a 0.709 median time ratio for scoped
256-record dispatch (29.1% less time), with unchanged transaction allocations.
One 4096-key long-window control records 1.053066, exceeding the 1.05 limit.
Reject the runtime change without rounding away that failure. Retain the source,
regression tests and all measurements for future controlled-host investigation.
