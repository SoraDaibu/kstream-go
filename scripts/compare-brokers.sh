#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
result=${1:-results/broker-comparison}
if (( $# > 0 )); then shift; fi
mkdir -p "$result"
result=$(cd "$result" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
go build -o "$work/app" ./cmd/broker-app
go build -o "$work/driver" ./cmd/broker-driver
mvn -q -f integration/kafka-streams/pom.xml compile dependency:build-classpath "-Dmdep.outputFile=$work/classpath"
classpath="$(pwd)/integration/kafka-streams/target/classes:$(cat "$work/classpath")"
python3 scripts/compare-brokers.py --go-app "$work/app" --driver "$work/driver" \
  --classpath "$classpath" --output "$result" --brokers "${KSTREAM_KAFKA_BROKERS:-localhost:9092}" "$@"
