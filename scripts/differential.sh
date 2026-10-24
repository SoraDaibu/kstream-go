#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if ! go version 2>/dev/null | grep -q '^go version go1\.'; then
  echo 'A Go programming-language toolchain >=1.27 is required.' >&2
  exit 1
fi
command -v mvn >/dev/null
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
python3 scripts/generate-traces.py "$work"
go build -o "$work/trace" ./cmd/trace
mvn -q -f integration/kafka-streams/pom.xml compile
for scenario in filter filter-not branch branch-default mapvalues flatmap flatmapvalues merge repartition-values repartition-count count reduce aggregate aggregate-null table join join-null left-join window hopping hopping-uneven window-reduce hopping-reduce hopping-uneven-reduce window-aggregate window-aggregate-null hopping-aggregate hopping-aggregate-null hopping-uneven-aggregate hopping-uneven-aggregate-null table-filter table-filter-not table-mapvalues table-transform-join metadata metadata-fanout process-typed process-typed-fanout; do
  mvn -q -f integration/kafka-streams/pom.xml exec:java \
    -Dexec.mainClass=dev.kstream.Oracle \
    "-Dexec.args=$scenario $work/$scenario.tsv $work/$scenario.java.tsv"
  for batch in 1 32 256; do
    for group in 0 1; do
      for capacity in 0 256; do
        KSTREAM_OUTPUT_CAPACITY=$capacity KSTREAM_TRANSACTION_GROUP=$group KSTREAM_BATCH_SIZE=$batch "$work/trace" "$scenario" "$work/$scenario.tsv" > "$work/$scenario.go.tsv"
        diff -u "$work/$scenario.java.tsv" "$work/$scenario.go.tsv"
        KSTREAM_OUTPUT_CAPACITY=$capacity KSTREAM_TRANSACTION_GROUP=$group KSTREAM_STATE_DIR="$work/state-$scenario-$batch-$group-$capacity" KSTREAM_BATCH_SIZE=$batch "$work/trace" "$scenario" "$work/$scenario.tsv" > "$work/$scenario.pebble.tsv"
        diff -u "$work/$scenario.java.tsv" "$work/$scenario.pebble.tsv"
      done
    done
  done
  echo "PASS: $scenario (seed 73019; batches 1,32,256; capacities 0,256; memory/Pebble; independent/group task commits)"
done
