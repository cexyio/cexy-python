#!/usr/bin/env bash
# Regenerate all generated code. CI runs this and fails if `git diff` is not empty.
#   1. cexy/_generated/  models + operation table from spec/openapi.sdk.json
#   2. cexy/_sync/       synchronous code from cexy/_async/ (unasync)
set -euo pipefail
cd "$(dirname "$0")/.."
python scripts/generate.py
python scripts/unasync.py
