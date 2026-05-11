#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
DB_FILE="${REPO_ROOT}/Crawl/db/ai_news.db"
WORKSHOP_STATE_DIR="${REPO_ROOT}/WorkShop/state"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

warn() {
  echo "WARN: $*" >&2
}

ok() {
  echo "OK: $*"
}

[[ -f "${ENV_FILE}" ]] || fail "Missing ${ENV_FILE}. Copy deploy/.env.example to deploy/.env first, or set GitHub secret BACKEND_ENV_FILE so deploy can install it automatically."

set -a
source "${ENV_FILE}"
set +a

DOCKERHUB_MIRROR_PREFIX="${DOCKERHUB_MIRROR_PREFIX-docker.m.daocloud.io/library/}"
export COMPOSE_PARALLEL_LIMIT="${COMPOSE_PARALLEL_LIMIT:-1}"
export NEXT_TELEMETRY_DISABLED="${NEXT_TELEMETRY_DISABLED:-1}"
export OPENMAIC_NODE_OPTIONS="${OPENMAIC_NODE_OPTIONS:---max-old-space-size=1024}"
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

command -v docker >/dev/null 2>&1 || fail "docker is not installed or not in PATH."
docker info >/dev/null 2>&1 || fail "docker daemon is not running or current user cannot access it."
docker compose version >/dev/null 2>&1 || fail "docker compose plugin is not available."

required_dirs=(
  "${REPO_ROOT}/Agent-Do"
  "${REPO_ROOT}/Crawl"
  "${REPO_ROOT}/OpenMAIC"
  "${REPO_ROOT}/WorkShop"
  "${REPO_ROOT}/deploy"
)

for dir in "${required_dirs[@]}"; do
  [[ -d "${dir}" ]] || fail "Missing required directory: ${dir}"
done

openmaic_env_file="${REPO_ROOT}/OpenMAIC/.env.local"
openmaic_env_example="${REPO_ROOT}/OpenMAIC/.env.example"
if [[ ! -f "${openmaic_env_file}" ]]; then
  if [[ -f "${openmaic_env_example}" ]]; then
    cp "${openmaic_env_example}" "${openmaic_env_file}"
    warn "Missing OpenMAIC env file; copied ${openmaic_env_example} to ${openmaic_env_file}."
  else
    fail "Missing OpenMAIC env file and template: ${openmaic_env_file}"
  fi
fi

[[ -n "${AGENT_DATA_HOST_ROOT:-}" ]] || fail "AGENT_DATA_HOST_ROOT is required."
[[ -n "${ALIYUN_ANTHROPIC_API_KEY:-${DASHSCOPE_API_KEY:-}}" ]] || warn "ALIYUN_ANTHROPIC_API_KEY or DASHSCOPE_API_KEY is empty; Workshop generation will fail."

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

[[ -w "${WORKSHOP_STATE_DIR}" ]] || fail "Workshop state directory is not writable: ${WORKSHOP_STATE_DIR}"
[[ -w "${AGENT_DATA_HOST_ROOT}" ]] || fail "Agent data host root is not writable: ${AGENT_DATA_HOST_ROOT}"
[[ -f "${REPO_ROOT}/Agent-Do/Dockerfile.claude" ]] || fail "Missing Claude runtime Dockerfile: ${REPO_ROOT}/Agent-Do/Dockerfile.claude"

docker compose -f "${SCRIPT_DIR}/docker-compose.yml" --env-file "${ENV_FILE}" --profile crawler --profile edurepo config --quiet

ok "Docker daemon is available."
ok "Required backend directories exist."
ok "SQLite path is a file: ${DB_FILE}"
ok "Workshop state directory is writable: ${WORKSHOP_STATE_DIR}"
ok "Agent data host root is writable: ${AGENT_DATA_HOST_ROOT}"
ok "Claude runtime Dockerfile exists."
ok "Compose config is valid."
ok "Cloud preflight finished. You can run ./deploy/up.sh next."
