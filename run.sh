#!/usr/bin/env bash
# Runs a Python script with dependencies installed: ./run.sh produce_messages.py
# Uses uv if available (recommended), otherwise falls back to venv + requirements.txt.
set -euo pipefail
cd "$(dirname "$0")"

if command -v uv >/dev/null 2>&1; then
  exec uv run --quiet "$@"
fi

[ -d venv ] || python3 -m venv venv
venv/bin/pip install -q -r requirements.txt
exec venv/bin/python "$@"
