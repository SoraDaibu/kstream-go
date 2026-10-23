#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
result=${1:-results/comparison}
mkdir -p "$result"
result=$(cd "$result" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
go version > "$result/environment.txt"
java -version 2>> "$result/environment.txt"
lscpu >> "$result/environment.txt"
go build -o "$work/benchmark" ./cmd/benchmark
mvn -q -f integration/kafka-streams/pom.xml compile dependency:build-classpath "-Dmdep.outputFile=$work/classpath"
classpath="$(pwd)/integration/kafka-streams/target/classes:$(cat "$work/classpath")"
python3 scripts/compare-engines.py "$work/benchmark" "$classpath" "$result"
