#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}. Copy deploy/.env.example to deploy/.env and fill real values first."
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

mkdir -p \
  "${REPO_ROOT}/Crawl/logs" \
  "${REPO_ROOT}/Crawl/db" \
  "${REPO_ROOT}/Crawl/static/page-shots" \
  "${REPO_ROOT}/Agent-Do/data" \
  "${REPO_ROOT}/WorkShop/state" \
  "${REPO_ROOT}/EduRepo/backend/data"

if [[ ! -f "${REPO_ROOT}/Crawl/db/ai_news.db" ]]; then
  touch "${REPO_ROOT}/Crawl/db/ai_news.db"
fi

CLAUDE_IMAGE="${CLAUDE_DOCKER_IMAGE:-claude-runtime:latest}"
docker build -t "${CLAUDE_IMAGE}" -f "${REPO_ROOT}/Agent-Do/Dockerfile.claude" "${REPO_ROOT}/Agent-Do"

docker compose -f "${SCRIPT_DIR}/docker-compose.yml" --env-file "${ENV_FILE}" up -d --build "$@"
