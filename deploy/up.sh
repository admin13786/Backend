#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
DB_FILE="${REPO_ROOT}/Crawl/db/ai_news.db"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}. Copy deploy/.env.example to deploy/.env and fill real values first."
  exit 1
fi

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

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

if [[ -d "${DB_FILE}" ]]; then
  fail "Expected SQLite file at ${DB_FILE}, but found a directory. Remove or rename that directory first."
fi

if [[ -e "${DB_FILE}" && ! -f "${DB_FILE}" ]]; then
  fail "Expected SQLite file at ${DB_FILE}, but found an unsupported path type."
fi

if [[ ! -f "${DB_FILE}" ]]; then
  touch "${DB_FILE}"
fi

CLAUDE_IMAGE="${CLAUDE_DOCKER_IMAGE:-claude-runtime:latest}"
docker build \
  --build-arg NODE_IMAGE="${CLAUDE_NODE_IMAGE:-node:20-slim}" \
  --build-arg APT_MIRROR="${CLAUDE_APT_MIRROR:-http://mirrors.aliyun.com/debian}" \
  --build-arg APT_FALLBACK_MIRROR="${CLAUDE_APT_FALLBACK_MIRROR:-http://mirrors.tuna.tsinghua.edu.cn/debian}" \
  --build-arg NPM_REGISTRY="${CLAUDE_NPM_REGISTRY:-https://registry.npmmirror.com}" \
  -t "${CLAUDE_IMAGE}" \
  -f "${REPO_ROOT}/Agent-Do/Dockerfile.claude" \
  "${REPO_ROOT}/Agent-Do"

docker compose -f "${SCRIPT_DIR}/docker-compose.yml" --env-file "${ENV_FILE}" up -d --build "$@"
