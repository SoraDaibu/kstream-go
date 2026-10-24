# Processor metadata tracking on Go 1.27.3

Baseline: source `aa0cff0392af8b0444f0e9488df47854ca737962` (local
`2b7b4d2`). After: optional source-handler wrappers in this change. Go 1.27.3,
Linux/amd64, EPYC 9V74. No broker or durable commit is measured here.

The first active-input-slice design made the single-record wrapper's temporary
array escape: 22 → 23 allocations, +96 bytes per record. Its raw samples are
retained as a rejected experiment. The final design stores scalar source fields
only in enabled tasks, wrapping their handlers at setup. The DSL enables it for
public custom Process factories; built-in-only topologies keep ordinary handlers.
Raw runtime callers opt in with WithRecordMetadata.

Existing allocation counts stay unchanged in all eight transaction-batch cases
and all four single-record window cases (22 allocations each). No metadata field
is added to every typed or byte record, and no input array is retained. The
initial broad samples use:

```sh
go test ./execution -run '^$' -bench BenchmarkTransactionBatch -benchmem -benchtime=300ms -count=5 -cpu=1
go test ./stream -run '^$' -bench BenchmarkWindowExpiry -benchmem -benchtime=200ms -count=3 -cpu=1
```

Short sequential timings varied, so a longer follow-up used two worktrees,
five alternating before/after pairs, one second per case, pinned to the same
logical CPU. Exact CPU and every raw command result are in paired.txt. This is
still a shared host; affinity does not isolate physical CPUs. Cases were
stateful batch16, stateless batch1024, and single-record windows with 64 keys and
size 2^60. Both worktrees were compiled before measurement.

| Follow-up case | Median paired after/before | Paired range |
|---|---:|---:|
| TransactionBatch/stateful=true/batch=16 | 1.004 | 0.957–1.037 |
| TransactionBatch/stateful=false/batch=1024 | 0.971 | 0.894–1.111 |
| WindowExpiry/keys=64/size=1152921504606846976 | 1.005 | 0.968–1.081 |

These ratios describe observed variation, not a speedup claim. Retain any
possible overhead; metadata is a feature addition. Broad before/after samples,
rejected-candidate allocation evidence and paired follow-up are separate files.

Enabled capture/getter use is measured separately with:

```sh
go test ./execution -run '^$' -bench BenchmarkRecordMetadataCapture -benchmem -benchtime=300ms -count=5 -cpu=1
```

| Inputs per batch | Median disabled ns/op | Median enabled ns/op | B/op / allocs/op (both) |
|---|---:|---:|---:|
| 1 | 294.7 | 294.6 | 320 / 3 |
| 256 | 12756.0 | 14432.0 | 320 / 3 |

Each enabled callback checks source identity/offset and total callback coverage;
disabled callbacks verify absence. Tracking/getting metadata has a CPU cost but
no additional per-record object allocation in these cases. Setup wrapper
allocation is outside the timed region. These measurements do not qualify Kafka
Streams production throughput, recovery or feature equivalence.

After source SHA256:
- `execution/task.go`: `437d2c76ec54de1c1cd17762a1370f900b5dcdcb1536c2b4f5f5114789774df6`
- `execution/options.go`: `1a5160bb329b5c6ff10d254a8f949bbb39b0f05cf8c14a51386a4a0d158a20da`
- `stream/stream.go`: `c3de9cb481dd9252826e00f4ed4eddbbf94c41fab4fd984974cb4fc0f538b769`
- `execution/metadata_bench_test.go`: `bacec314292027246cad54acab6ac14459f0bc5d21c4e4d7c1904c94bcc27dd0`
