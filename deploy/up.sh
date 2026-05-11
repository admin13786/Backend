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

if [[ -n "${OPENMAIC_PUBLIC_ORIGIN_OVERRIDE:-}" ]]; then
  export OPENMAIC_PUBLIC_ORIGIN="${OPENMAIC_PUBLIC_ORIGIN_OVERRIDE}"
fi
if [[ -n "${FRONTEND_VITE_OPENMAIC_BASE_URL_OVERRIDE:-}" ]]; then
  export FRONTEND_VITE_OPENMAIC_BASE_URL="${FRONTEND_VITE_OPENMAIC_BASE_URL_OVERRIDE}"
fi
if [[ -n "${FRONTEND_VITE_OPENMAIC_APP_URL_OVERRIDE:-}" ]]; then
  export FRONTEND_VITE_OPENMAIC_APP_URL="${FRONTEND_VITE_OPENMAIC_APP_URL_OVERRIDE}"
fi
if [[ -n "${OPENMAIC_NODE_OPTIONS_OVERRIDE:-}" ]]; then
  export OPENMAIC_NODE_OPTIONS="${OPENMAIC_NODE_OPTIONS_OVERRIDE}"
fi

DOCKERHUB_MIRROR_PREFIX="${DOCKERHUB_MIRROR_PREFIX-docker.m.daocloud.io/library/}"
export COMPOSE_PARALLEL_LIMIT="${COMPOSE_PARALLEL_LIMIT:-1}"
export NEXT_TELEMETRY_DISABLED="${NEXT_TELEMETRY_DISABLED:-1}"
export OPENMAIC_NODE_OPTIONS="${OPENMAIC_NODE_OPTIONS:---max-old-space-size=1536}"
export CRAWL_PYTHON_IMAGE="${CRAWL_PYTHON_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}python:3.10-slim}"
export AGENT_DO_PYTHON_IMAGE="${AGENT_DO_PYTHON_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}python:3.11-slim}"
export AGENT_DO_DOCKER_CLI_IMAGE="${AGENT_DO_DOCKER_CLI_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}docker:27-cli}"
export WORKSHOP_PYTHON_IMAGE="${WORKSHOP_PYTHON_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}python:3.11-slim}"
export CLAUDE_NODE_IMAGE="${CLAUDE_NODE_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}node:20-slim}"
export APP_RUNTIME_IMAGE="${APP_RUNTIME_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}node:20-alpine}"
export OPENMAIC_NODE_IMAGE="${OPENMAIC_NODE_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}node:22-alpine3.19}"
export WEB_FRONTEND_NODE_IMAGE="${WEB_FRONTEND_NODE_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}node:22-alpine}"
export WEB_FRONTEND_NGINX_IMAGE="${WEB_FRONTEND_NGINX_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}nginx:stable-alpine}"
export EDUREPO_PYTHON_IMAGE="${EDUREPO_PYTHON_IMAGE:-${DOCKERHUB_MIRROR_PREFIX}python:3.10-slim}"

ensure_swap() {
  [[ "${DEPLOY_SWAP_ENABLED:-1}" == "1" ]] || return 0
  [[ "$(uname -s 2>/dev/null || true)" == "Linux" ]] || return 0
  command -v swapon >/dev/null 2>&1 || return 0

  local swap_file="${DEPLOY_SWAP_FILE:-/swapfile-aiedu}"
  local swap_size_mb="${DEPLOY_SWAP_SIZE_MB:-4096}"

  if swapon --show=NAME --noheadings 2>/dev/null | grep -Fxq "${swap_file}"; then
    return 0
  fi

  if [[ "$(id -u)" -ne 0 ]]; then
    echo "WARN: DEPLOY_SWAP_ENABLED=1 but current user is not root; skipping swap setup." >&2
    return 0
  fi

  if [[ ! -f "${swap_file}" ]]; then
    fallocate -l "${swap_size_mb}M" "${swap_file}" 2>/dev/null || \
      dd if=/dev/zero of="${swap_file}" bs=1M count="${swap_size_mb}" status=none
    chmod 600 "${swap_file}"
    mkswap "${swap_file}" >/dev/null
  fi

  swapon "${swap_file}" 2>/dev/null || \
    echo "WARN: Failed to enable swap file ${swap_file}; continuing without swap." >&2
}

ensure_swap

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
