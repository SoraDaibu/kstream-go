#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if ! go version 2>/dev/null | grep -q '^go version go1\.'; then
  echo 'A Go programming-language toolchain >=1.23 is required.' >&2
  exit 1
fi
command -v mvn >/dev/null
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
python3 scripts/generate-traces.py "$work"
go build -o "$work/trace" ./cmd/trace
mvn -q -f integration/kafka-streams/pom.xml compile
for scenario in filter filter-not mapvalues flatmap flatmapvalues merge count table join window; do
  mvn -q -f integration/kafka-streams/pom.xml exec:java \
    -Dexec.mainClass=dev.kstream.Oracle \
    "-Dexec.args=$scenario $work/$scenario.tsv $work/$scenario.java.tsv"
  for batch in 1 32 256; do
    KSTREAM_BATCH_SIZE=$batch "$work/trace" "$scenario" "$work/$scenario.tsv" > "$work/$scenario.go.tsv"
    diff -u "$work/$scenario.java.tsv" "$work/$scenario.go.tsv"
    KSTREAM_STATE_DIR="$work/state-$scenario-$batch" KSTREAM_BATCH_SIZE=$batch "$work/trace" "$scenario" "$work/$scenario.tsv" > "$work/$scenario.pebble.tsv"
    diff -u "$work/$scenario.java.tsv" "$work/$scenario.pebble.tsv"
  done
  echo "PASS: $scenario (seed 73019; transaction batches 1,32,256; memory and Pebble stores)"
done
