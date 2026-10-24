#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if ! go version 2>/dev/null | grep -q '^go version go1\.'; then
  echo 'A Go programming-language toolchain >=1.27 is required.' >&2
  exit 1
fi
mapfile -t files < <(find . -name '*.go' -not -path './.git/*')
gofmt -w "${files[@]}"
