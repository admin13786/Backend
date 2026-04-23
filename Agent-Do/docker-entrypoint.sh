#!/usr/bin/env bash
set -euo pipefail

CLAUDE_IMAGE="${CLAUDE_DOCKER_IMAGE:-claude-runtime:latest}"
DOCKER_BIN="${DOCKER_BIN:-docker}"

if ! "${DOCKER_BIN}" image inspect "${CLAUDE_IMAGE}" >/dev/null 2>&1; then
  echo "[agent-do] warning: ${CLAUDE_IMAGE} not found; build it on the host before sending Claude requests"
fi

exec uvicorn app.main:app --host 0.0.0.0 --port 8000
