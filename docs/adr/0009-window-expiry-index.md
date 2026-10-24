# ADR 0009: Derived task-local window expiry index

Status: Accepted for the experimental tumbling-count implementation after benchmark and oracle validation.

The baseline tumbling count scans and copies the complete window store every
time operator-local stream-time advances. With many retained keys, the cleanup
cost grows with all live state even when no window can expire. It affects both
memory stores and persistent stores and cannot satisfy the performance goal for
large windows.

Maintain a typed min-heap of (window end, encoded store key) inside each compiled
window processor. Build it once from committed task state on first use. Insert
only when a window/key is created, and pop entries as the clock closes them.
Stage deletions in the existing transaction; the heap is a derived index, not an
independent durability authority. It uses no reflection, interface boxing,
per-operator concurrency or locks.

The index may change before a backend commit succeeds. That is safe only because
every callback, store or ambiguous commit failure poisons the task and its
compiled processors. A fresh task must rebuild the index from the committed
store. Do not reuse the old heap after rollback or recover it from nontransactional
external state. Single task ownership and shared graph compilation remain
required invariants.

This changes steady-state cleanup from scanning every retained entry on each
clock advance to O(log N) per created/expired entry. Recovery still scans all
entries once and retains O(N) heap memory. A persistent ordered expiry index and
streamed restore remain candidates for larger-than-RAM windows; this decision
does not establish bounded recovery memory or production parity.

Compare identical fixed-length input workloads before/after with raw data,
source, environment and commands. Cover many live keys and actual rollover.
Verify staged deletions, recovery, replay, failed commits, null keys, empty keys,
grace equality and timestamp overflow. Run the Java differential oracle against
both store implementations before accepting the optimization.
