#!/usr/bin/env bash
# 在浏览器中打开「对话预填」接口，写入 Cookie 并跳转到主页，输入框会显示预填文案。
#
# 实现方式：GET /api/dialog-prefill?title=...&to=home → 302 到 /?prefill=...（同一次浏览器请求会带上 Set-Cookie）。
#
# 用法:
#   ./scripts/open-home-with-dialog-prefill.sh
#   OPENMAIC_URL=http://127.0.0.1:3001 PREFILL_TITLE='自定义标题' ./scripts/open-home-with-dialog-prefill.sh

set -euo pipefail

BASE_URL="${OPENMAIC_URL:-http://localhost:3000}"
TITLE="${PREFILL_TITLE:-测试中}"

ENC_TITLE="$(node -e "console.log(encodeURIComponent(process.argv[1]))" -- "$TITLE")"

URL="${BASE_URL}/api/dialog-prefill?title=${ENC_TITLE}&to=home"
echo "打开: ${URL}"

if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$URL" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
  open "$URL" >/dev/null 2>&1 || true
else
  echo "未找到 xdg-open/open，请手动在浏览器打开上述 URL。" >&2
  exit 1
fi
