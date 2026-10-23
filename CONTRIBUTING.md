# Contributing

This is an early experimental project. Keep the public API small and the
implementation idiomatic Go. Changes are licensed under Apache-2.0.

1. Read the architecture ADRs and compatibility matrix.
2. Run `gofmt -w` on changed Go files, `go vet ./...`, and `go test -race ./...`.
3. Run the Java differential suite for semantic changes. Add a minimized
   regression trace for each divergence; document deliberate incompatibilities.
4. Include benchmark evidence for hot-path optimization or scheduling changes.
5. State what was actually validated and which production gates remain open.

Do not mechanically port Kafka Streams Java internals. Avoid reflection,
per-operator goroutines/channels, hidden shared state and unbounded queues.
Processor factories run per task; callbacks must not use nontransactional
side effects as a substitute for state stores or output sinks.

Be respectful, assume good intent, and make technical disagreements specific.
Do not submit credentials, private data, or generated benchmark claims.
