# Same-host fetch-budget experiment

Source `7b76498ba7a45cb6d0455a36772f98f46996853c` passed
[all 18 verified Go-only cases](https://github.com/SoraDaibu/kstream-go/actions/runs/40095814558)
plus ordinary, 864 reference, storage, broker fault and paired/profile gates.
Go 1.27.3, Kafka 3.9.1, three rotating balanced rounds on one runner, identical
binaries, 1M warmup and 1M measured inputs per fresh process; filter/count,
3 partitions, 64 keys, poll limit 4096, output capacity 4096, no compression.

Ratios compare each Go target with 8 MiB in the same round. They are the median
of paired ratios, not ratios of absolute medians, and do not compare with Kafka
Streams. Every case verifies exact order/count/checksum, extra tails, actual
committed group offsets and batch-phase counts.

| Workload/target | Median paired throughput ratio | Paired range | Median paired RSS ratio | Median peak RSS KiB | Median records/batch |
|---|---:|---:|---:|---:|---:|
| Filter / 8 MiB | 1.000 | 1.000–1.000 | 1.000 | 231092 | 4032 |
| Filter / 1 MiB | 1.001 | 0.956–1.407 | 0.293 | 67704 | 3861 |
| Filter / 256 KiB | 0.965 | 0.939–1.498 | 0.176 | 40988 | 3846 |
| Count / 8 MiB | 1.000 | 1.000–1.000 | 1.000 | 227864 | 4032 |
| Count / 1 MiB | 1.127 | 0.951–1.212 | 0.295 | 70304 | 3861 |
| Count / 256 KiB | 1.021 | 0.766–1.133 | 0.192 | 43692 | 3831 |

The 1 MiB target consistently lowers RSS in these tiny-value fixtures, with no
clear filter throughput gain and exploratory count improvement. One count round
still regresses. The 256 KiB target saves more memory, but count loses 23% in one
round and observed CPU rises there. Neither result establishes a universal
throughput improvement. Smaller fetches also reduce average transaction density.

Use 1 MiB for a separate five-pair Kafka comparison/profile follow-up on these
known fixtures. Library, app and script defaults remain 8 MiB. Fetch targets are
not hard memory/oversized/compressed-batch limits. Larger values, heterogeneous
inputs, HA/recovery, common hard memory caps and isolated resources remain open.
Three rounds, one broker, small state and shared CPUs are exploratory evidence;
no production qualification or 2.5x Kafka target is claimed.

The complete original archive retains all 18 leaf samples/manifests/environments,
commands and logs. Verified SHA256:
`707fb7ffd3d66f30176c9aed59a6286223fd49e333aedcdbe89bfa01d96c832c`.
