#!/usr/bin/env bash
# Copy the API contract from a cexy-api-spec checkout (default: ../cexy-api-spec).
#   scripts/sync_spec.sh [path]           copy spec + conformance fixtures
#   scripts/sync_spec.sh [path] --check   fail if the vendored copies differ
set -euo pipefail
cd "$(dirname "$0")/.."
SRC="${1:-../cexy-api-spec}"
MODE="${2:-}"
pairs=(
  "spec/openapi.sdk.json:spec/openapi.sdk.json"
  "errors.yaml:spec/errors.yaml"
  "asyncapi.yaml:spec/asyncapi.yaml"
  "conformance:tests/fixtures/conformance"
)
status=0
for pair in "${pairs[@]}"; do
  from="$SRC/${pair%%:*}"; to="${pair##*:}"
  if [[ "$MODE" == "--check" ]]; then
    diff -r "$from" "$to" >/dev/null || { echo "out of sync: $to (from $from)"; status=1; }
  else
    rm -rf "$to"; cp -r "$from" "$to"
  fi
done
exit $status
