# Produce completion: real broker evidence

Both sources passed all ordinary, 720 independent reference, persistent-state,
four broker fault and verified comparison/profile gates:

- Before: `908680e75a4f99703a53a04242122980f4e94ce4`,
  [run 40085730770](https://github.com/SoraDaibu/kstream-go/actions/runs/40085730770).
- After: `636abbe45029b624c48661ea4bc6858ef05e8855`,
  [run 40086116894](https://github.com/SoraDaibu/kstream-go/actions/runs/40086116894).

Go 1.27.3, Java 17, Kafka/Kafka Streams 3.9.1; 1M warmup and 1M measured inputs,
five fresh-process alternating engine pairs per workload, output capacity 4096.

| Phase/workload | Median paired Go/Kafka throughput | Paired range | Go/Kafka median records/s | Go/Kafka median peak RSS KiB |
|---|---:|---:|---:|---:|
| Before/filter | 2.26x | 1.53–2.73x | 436262 / 191906 | 232916 / 154712 |
| After/filter | 2.20x | 1.53–2.53x | 617506 / 280668 | 229396 / 160288 |
| Before/count | 7.34x | 5.72–7.60x | 453843 / 60949 | 253004 / 228464 |
| After/count | 5.95x | 5.40–6.02x | 743288 / 127105 | 243352 / 236344 |

The host changed from EPYC 7763 to EPYC 9V45. Both engines' absolute rates rose
and paired ratios fell. These are not controlled before/after throughput samples;
do not attribute rate changes to produce completion. Filter remains below the
2.5x target, and Go RSS remains higher in both workloads.

Separate filter profiles estimate 1643.66MB before and 1633.59MB after cumulative
allocation, about 0.6% lower overall. ProduceSync's 50.24MB flat allocation is
absent from the new profile; produceAll's constant completion state is below the
report threshold. Kafka client buffers/record decoding and output arrays remain
large costs. Eliminating the targeted result list does not imply a proportional
whole-process reduction; other allocations/sampling/runner effects offset it.
Profiles include startup/warmup/measurement/shutdown and are neither peak RSS
nor comparison timing samples. Local completion-model tiny-batch regressions
remain in the [isolated report](../../../docs/benchmarks/2026-10-24-produce-completion/README.md).

Every sample verifies exact output order/count/checksum, extra tails and actual
committed group offsets. Shared CPUs, one broker, replication factor one, tiny
state, different commit/cache policies and no common hard RSS cap leave production
qualification open. Full feature parity is also incomplete.

Each phase retains selected environment/manifest/sample/summary/profile text and
complete original archives. SHA256 verified against Actions metadata:

- before comparison: `c1da4f86737e2b984229bc1654fd51a4354beaf347c9fb06c2cbd9e560a218a5`
- before profiles: `104f69214bfe531d3e2c6fe85bd37a26549548e03834c7ee56cfe88c0e72aa56`
- after comparison: `d85cfb9d4c60a2be4c4ba846e10192d8d27b8fc30cc6089932a050d39e7693c6`
- after profiles: `1b413859f5a424ee7a787c905474b4ca088e5881651b1351104de91a8870b09a`
