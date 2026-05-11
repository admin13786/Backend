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
if [[ -n "${DEPLOY_SWAP_SIZE_MB_OVERRIDE:-}" ]]; then
  export DEPLOY_SWAP_SIZE_MB="${DEPLOY_SWAP_SIZE_MB_OVERRIDE}"
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
  local min_swap_mb="${DEPLOY_SWAP_MIN_SIZE_MB:-1024}"
  local reserve_mb="${DEPLOY_SWAP_DISK_RESERVE_MB:-1024}"
  local current_bytes=0
  local is_active=0
  local effective_size_mb="${swap_size_mb}"
  local available_mb=0

  if swapon --show=NAME --noheadings 2>/dev/null | grep -Fxq "${swap_file}"; then
    is_active=1
  fi

  if [[ -f "${swap_file}" ]]; then
    current_bytes="$(stat -c %s "${swap_file}" 2>/dev/null || echo 0)"
  fi

  if [[ "${is_active}" -eq 1 && "${current_bytes}" -ge $((swap_size_mb * 1024 * 1024)) ]]; then
    echo "OK: swap file is active: ${swap_file} (${swap_size_mb}MB target)."
    return 0
  fi

  if [[ "$(id -u)" -ne 0 ]]; then
    echo "WARN: DEPLOY_SWAP_ENABLED=1 but current user is not root; skipping swap setup." >&2
    return 0
  fi

  if [[ "${is_active}" -eq 1 ]]; then
    swapoff "${swap_file}" 2>/dev/null || {
      echo "WARN: Failed to disable undersized swap file ${swap_file}; continuing without resizing it." >&2
      return 0
    }
    rm -f "${swap_file}"
  fi

  if [[ "${is_active}" -eq 0 && -f "${swap_file}" ]]; then
    # Remove stale or partial files left by interrupted deployments.
    rm -f "${swap_file}"
  fi

  available_mb="$(
    df -Pm "$(dirname "${swap_file}")" 2>/dev/null | awk 'NR == 2 {print $4}'
  )"
  available_mb="${available_mb:-0}"

  if [[ "${available_mb}" -gt "${reserve_mb}" ]]; then
    local max_swap_mb=$((available_mb - reserve_mb))
    if [[ "${effective_size_mb}" -gt "${max_swap_mb}" ]]; then
      effective_size_mb="${max_swap_mb}"
      echo "WARN: Not enough free disk for ${swap_size_mb}MB swap; using ${effective_size_mb}MB instead." >&2
    fi
  fi

  if [[ "${effective_size_mb}" -lt "${min_swap_mb}" ]]; then
    echo "WARN: Free disk is too low to create a safe swap file; need at least ${min_swap_mb}MB after ${reserve_mb}MB reserve." >&2
    return 0
  fi

  if [[ ! -f "${swap_file}" ]]; then
    if ! fallocate -l "${effective_size_mb}M" "${swap_file}" 2>/dev/null; then
      if ! dd if=/dev/zero of="${swap_file}" bs=1M count="${effective_size_mb}" status=none; then
        rm -f "${swap_file}"
        echo "WARN: Failed to allocate swap file ${swap_file}; continuing without swap." >&2
        return 0
      fi
    fi
    chmod 600 "${swap_file}"
    if ! mkswap "${swap_file}" >/dev/null; then
      rm -f "${swap_file}"
      echo "WARN: Failed to initialize swap file ${swap_file}; continuing without swap." >&2
      return 0
    fi
  fi

  swapon "${swap_file}" 2>/dev/null || \
    echo "WARN: Failed to enable swap file ${swap_file}; continuing without swap." >&2

  if swapon --show=NAME --noheadings 2>/dev/null | grep -Fxq "${swap_file}"; then
    echo "OK: enabled swap file ${swap_file} (${effective_size_mb}MB)."
  fi
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
