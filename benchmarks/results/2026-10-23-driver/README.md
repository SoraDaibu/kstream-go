# Exploratory driver comparison

Source: `38f96e31a8b1e8364f06c5bc83e22e6972d0efe9`.
[Successful Actions run](https://github.com/SoraDaibu/kstream-go/actions/runs/40000342739).
See manifest.json, environment.txt, samples.csv and summary.json for the actual
configuration, raw trials and analysis. Run `./scripts/compare-engines.sh` to repeat.

Five alternating paired trials used 200,000 measured records after 100,000
warmup records. All paired output checksums matched.

| Scenario | Median paired throughput ratio (Go / Kafka driver) | Exploratory 95% bootstrap interval |
|---|---|---|
| Filter | 12.76x | 12.15x–12.80x |
| Keyed count | 9.12x | 8.81x–9.46x |

These numbers compare different **in-process drivers with in-memory stores**.
They omit Kafka brokers, durable state, production EOS, recovery and tail latency.
They are promising engineering signals, not evidence that the production 2.5x
target is achieved. `production_qualified` remains false. See
[comparison methodology](../../README.md) and the full qualification criteria.
