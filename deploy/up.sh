#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
DB_FILE="${REPO_ROOT}/Crawl/db/ai_news.db"
WORKSHOP_STATE_DIR="${REPO_ROOT}/WorkShop/state"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}. Copy deploy/.env.example to deploy/.env and fill real values first, or inject BACKEND_ENV_FILE from GitHub Actions."
  exit 1
fi

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

set -a
source "${ENV_FILE}"
set +a

[[ -n "${AGENT_DATA_HOST_ROOT:-}" ]] || fail "AGENT_DATA_HOST_ROOT is required."

OPENMAIC_ENV_FILE="${REPO_ROOT}/OpenMAIC/.env.local"
OPENMAIC_ENV_EXAMPLE="${REPO_ROOT}/OpenMAIC/.env.example"
if [[ ! -f "${OPENMAIC_ENV_FILE}" ]]; then
  if [[ -f "${OPENMAIC_ENV_EXAMPLE}" ]]; then
    cp "${OPENMAIC_ENV_EXAMPLE}" "${OPENMAIC_ENV_FILE}"
  else
    fail "Missing OpenMAIC env file and template: ${OPENMAIC_ENV_FILE}"
  fi
fi

mkdir -p \
  "${AGENT_DATA_HOST_ROOT}" \
  "${REPO_ROOT}/Crawl/logs" \
  "${REPO_ROOT}/Crawl/db" \
  "${REPO_ROOT}/Crawl/static/page-shots" \
  "${REPO_ROOT}/Agent-Do/data" \
  "${WORKSHOP_STATE_DIR}" \
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

if [[ ! -w "${WORKSHOP_STATE_DIR}" ]]; then
  fail "Workshop state directory is not writable: ${WORKSHOP_STATE_DIR}"
fi

if [[ ! -w "${AGENT_DATA_HOST_ROOT}" ]]; then
  fail "Agent data host root is not writable: ${AGENT_DATA_HOST_ROOT}"
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

docker image inspect "${CLAUDE_IMAGE}" >/dev/null 2>&1 || fail "Required Claude runtime image was not built: ${CLAUDE_IMAGE}"

docker compose -f "${SCRIPT_DIR}/docker-compose.yml" --env-file "${ENV_FILE}" up -d --build "$@"
