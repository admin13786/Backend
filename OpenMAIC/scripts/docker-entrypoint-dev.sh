#!/bin/sh
set -e
cd /app
# Bind mount: node_modules live on the host disk; first install can take many minutes.
echo "[openmaic-dev] Running pnpm install — Next will NOT listen on :3000 until this finishes."
echo "[openmaic-dev] Tip: run 'pnpm install' on the host in this folder first to speed this up."
pnpm install --frozen-lockfile --reporter=append-only
echo "[openmaic-dev] Starting next dev on 0.0.0.0:3000 ..."
exec pnpm exec next dev --hostname 0.0.0.0 --port 3000
