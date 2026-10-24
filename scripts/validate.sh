#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if ! go version 2>/dev/null | grep -q '^go version go1\.'; then
  echo 'A Go programming-language toolchain >=1.27 is required.' >&2
  exit 1
fi
mapfile -t files < <(find . -name '*.go' -not -path './.git/*')
unformatted=$(gofmt -l "${files[@]}")
if [[ -n "$unformatted" ]]; then
  echo 'Run ./scripts/format.sh first:' >&2
  echo "$unformatted" >&2
  exit 1
fi
python3 scripts/check-layers.py
go vet ./...
go test -race -coverprofile=coverage.out ./...
go run ./examples/wordcount
