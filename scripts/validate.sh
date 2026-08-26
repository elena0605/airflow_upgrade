#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "==> Python syntax check (dags/)"
python3 -m compileall -q dags/

echo "==> Docker Compose config check"
docker compose config -q

echo "==> Validation passed"
