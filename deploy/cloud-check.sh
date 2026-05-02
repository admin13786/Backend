#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
DB_FILE="${REPO_ROOT}/Crawl/db/ai_news.db"

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

[[ -f "${ENV_FILE}" ]] || fail "Missing ${ENV_FILE}. Copy deploy/.env.example to deploy/.env first."

set -a
source "${ENV_FILE}"
set +a

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

[[ -n "${AGENT_DATA_HOST_ROOT:-}" ]] || fail "AGENT_DATA_HOST_ROOT is required."
[[ -n "${ALIYUN_ANTHROPIC_API_KEY:-${DASHSCOPE_API_KEY:-}}" ]] || warn "ALIYUN_ANTHROPIC_API_KEY or DASHSCOPE_API_KEY is empty; Workshop generation will fail."

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

docker compose -f "${SCRIPT_DIR}/docker-compose.yml" --env-file "${ENV_FILE}" --profile crawler --profile edurepo config --quiet

ok "Docker daemon is available."
ok "Required backend directories exist."
ok "SQLite path is a file: ${DB_FILE}"
ok "Compose config is valid."
ok "Cloud preflight finished. You can run ./deploy/up.sh next."
