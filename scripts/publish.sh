#!/usr/bin/env bash
# Explicitly creates a PUBLIC GitHub repository after local validation succeeds.
set -euo pipefail
cd "$(dirname "$0")/.."
repo=${1:-SoraDaibu/kstream-go}
if [[ ! "$repo" =~ ^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+$ ]]; then
  echo 'Usage: ./scripts/publish.sh owner/repository' >&2
  exit 1
fi
if [[ -n "$(git status --porcelain)" ]]; then
  echo 'Commit local changes before publishing.' >&2
  exit 1
fi
if [[ "$(git symbolic-ref --short HEAD)" != main ]]; then
  echo 'Publish from main.' >&2
  exit 1
fi
./scripts/validate.sh
./scripts/differential.sh
if git remote get-url origin >/dev/null 2>&1; then
  echo 'An origin already exists; inspect it before publishing.' >&2
  exit 1
fi
# GitHub auth must already be configured; never read or print token values.
gh auth status
gh repo create "$repo" --public --source=. --remote=origin \
  --description 'Experimental Go-native stream processing inspired by Kafka Streams' --push
