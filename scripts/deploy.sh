#!/usr/bin/env bash
# Run on the deployment host (manually or from GitLab CI) to rebuild and restart the stack.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BRANCH="${DEPLOY_BRANCH:-main}"
COMPOSE="${COMPOSE_CMD:-docker compose}"

echo "==> Deploying from ${ROOT_DIR} (branch: ${BRANCH})"

if [[ -d .git ]]; then
  git fetch origin
  git checkout "$BRANCH"
  git pull origin "$BRANCH"
fi

if [[ ! -f .env ]]; then
  echo "ERROR: .env is missing. Copy .env.example to .env and configure secrets on this host."
  exit 1
fi

mkdir -p logs config data/tmp_openai_batches/youtube_thumbnail data/tmp_openai_batches/tiktok_thumbnail data/tmp_openai_batches/tiktok_comments

echo "==> Building images"
$COMPOSE build

echo "==> Running database / Airflow init (if needed)"
$COMPOSE up airflow-init

echo "==> Starting services"
$COMPOSE up -d --remove-orphans

echo "==> Service status"
$COMPOSE ps

echo "==> Done. Airflow UI: http://<host>:8080"
