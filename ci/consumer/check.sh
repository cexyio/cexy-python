#!/usr/bin/env bash
# Install each built distribution into its own fresh venv, with no extras and no dev
# dependencies, and run the smoke check from outside the repository.
#   ci/consumer/check.sh <dist-dir>
set -euo pipefail
DIST="$(cd "$1" && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"
VERSION="$(python3 -c "import re;print(re.search(r'^version = \"(.+)\"', open('$HERE/../../pyproject.toml').read(), re.M).group(1))")"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
for kind in whl tar.gz; do
  files=("$DIST"/*."$kind")
  [ "${#files[@]}" -eq 1 ] && [ -f "${files[0]}" ] || { echo "expected exactly one .$kind in $DIST"; exit 1; }
  python3 -m venv "$WORK/$kind"
  "$WORK/$kind/bin/pip" install -q --disable-pip-version-check "${files[0]}"
  (cd "$WORK" && "$WORK/$kind/bin/python" "$HERE/smoke.py" "$VERSION")
done
