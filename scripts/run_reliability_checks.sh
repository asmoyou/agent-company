#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$ROOT/.venv/bin/python"

if [[ ! -x "$PY" ]]; then
  echo "missing python runtime: $PY"
  exit 1
fi

cd "$ROOT"

echo "[1/2] compile checks"
"$PY" -m compileall server agents

echo "[2/2] unit tests"
"$PY" -m unittest discover -s tests -p 'test_*.py' -v

echo "reliability checks passed"
