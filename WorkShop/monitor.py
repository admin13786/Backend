"""
Workshop 性能监控仪表盘  ·  端口 11000

启动:
    python monitor.py
    # 或
    uvicorn monitor:app --host 0.0.0.0 --port 11000

提供:
    GET  /                          HTML 仪表盘
    GET  /api/stats?hours=24        各端点聚合统计
    GET  /api/step-stats?endpoint=…&hours=24  各步骤聚合
    GET  /api/requests?limit=50&endpoint=…    最近请求列表
    GET  /api/request/{request_id}            单请求详情（含步骤）
    POST /api/purge?hours=72                  清理旧数据
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from metrics import (
    purge_old,
    query_recent_requests,
    query_request_with_steps,
    query_stats,
    query_step_stats,
    query_stream_events,
)

app = FastAPI(title="Workshop Monitor", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ───────────────────────────── API ─────────────────────────────


@app.get("/api/stats")
async def api_stats(hours: int = Query(24, ge=1, le=720)):
    return query_stats(hours)


@app.get("/api/step-stats")
async def api_step_stats(endpoint: str, hours: int = Query(24, ge=1, le=720)):
    return query_step_stats(endpoint, hours)


@app.get("/api/requests")
async def api_requests(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    endpoint: str | None = None,
):
    rows = query_recent_requests(limit=limit, endpoint=endpoint, offset=offset)
    return rows


@app.get("/api/request/{request_id}")
async def api_request_detail(request_id: str):
    data = query_request_with_steps(request_id)
    if data is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    return data


@app.get("/api/conversation/{request_id}")
async def api_conversation(request_id: str):
    events = query_stream_events(request_id)
    if not events:
        return JSONResponse({"error": "no conversation events found"}, status_code=404)
    return {"request_id": request_id, "events": events}


@app.post("/api/purge")
async def api_purge(hours: int = Query(72, ge=1)):
    n = purge_old(hours)
    return {"purged": n}


# ───────────────────────────── Dashboard HTML ─────────────────────────────


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Workshop 性能监控</title>
<style>
:root {
  --bg: #0f1117;
  --surface: #1a1d27;
  --surface2: #242734;
  --border: #2e3144;
  --text: #e4e6f0;
  --text2: #8b8fa8;
  --accent: #6c8cff;
  --accent2: #4ecdc4;
  --green: #4ecdc4;
  --red: #ff6b6b;
  --orange: #ffa94d;
  --yellow: #ffe066;
  --font: 'JetBrains Mono', 'SF Mono', 'Fira Code', 'Consolas', monospace;
}
* { margin:0; padding:0; box-sizing:border-box; }
body {
  background: var(--bg);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  line-height: 1.6;
  min-height: 100vh;
}
.header {
  background: linear-gradient(135deg, #1a1d27 0%, #242734 100%);
  border-bottom: 1px solid var(--border);
  padding: 20px 32px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.header h1 {
  font-size: 20px;
  font-weight: 600;
  letter-spacing: -0.5px;
}
.header h1 span { color: var(--accent); }
.controls {
  display: flex;
  gap: 12px;
  align-items: center;
}
.controls select, .controls button {
  background: var(--surface2);
  color: var(--text);
  border: 1px solid var(--border);
  padding: 6px 14px;
  border-radius: 6px;
  font-size: 13px;
  cursor: pointer;
  transition: border-color .15s;
}
.controls select:hover, .controls button:hover {
  border-color: var(--accent);
}
.controls button.refresh {
  background: var(--accent);
  color: #fff;
  border-color: var(--accent);
  font-weight: 600;
}
.auto-tag {
  font-size: 11px;
  color: var(--text2);
  background: var(--surface2);
  padding: 2px 8px;
  border-radius: 10px;
}
.container { max-width: 1400px; margin: 0 auto; padding: 24px 32px; }

/* ─── Stats Cards ─── */
.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 16px;
  margin-bottom: 28px;
}
.stat-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 20px;
  transition: border-color .2s;
  cursor: pointer;
}
.stat-card:hover { border-color: var(--accent); }
.stat-card .ep-name {
  font-family: var(--font);
  font-size: 13px;
  color: var(--accent);
  margin-bottom: 12px;
  word-break: break-all;
}
.stat-card .metrics {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 8px;
}
.metric { text-align: center; }
.metric .val {
  font-family: var(--font);
  font-size: 22px;
  font-weight: 700;
}
.metric .label {
  font-size: 11px;
  color: var(--text2);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.metric.err .val { color: var(--red); }
.stat-card .extra {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 8px;
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid var(--border);
}
.extra .metric .val { font-size: 15px; }

/* ─── Step Breakdown ─── */
.step-section {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 20px;
  margin-bottom: 28px;
}
.step-section h3 {
  font-size: 14px;
  color: var(--text2);
  margin-bottom: 16px;
  font-weight: 500;
}
.step-section h3 code {
  color: var(--accent);
  font-family: var(--font);
  font-weight: 600;
}
.step-bar-row {
  display: flex;
  align-items: center;
  margin-bottom: 10px;
  gap: 12px;
}
.step-bar-row .step-label {
  flex: 0 0 200px;
  font-family: var(--font);
  font-size: 12px;
  text-align: right;
  color: var(--text2);
}
.step-bar-row .bar-track {
  flex: 1;
  height: 24px;
  background: var(--surface2);
  border-radius: 4px;
  position: relative;
  overflow: hidden;
}
.step-bar-row .bar-fill {
  height: 100%;
  border-radius: 4px;
  display: flex;
  align-items: center;
  padding-left: 8px;
  font-family: var(--font);
  font-size: 11px;
  font-weight: 600;
  color: #fff;
  white-space: nowrap;
  min-width: fit-content;
  transition: width .4s ease;
}
.step-bar-row .bar-stats {
  flex: 0 0 180px;
  font-family: var(--font);
  font-size: 11px;
  color: var(--text2);
}

/* ─── Request List ─── */
.req-section {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  overflow: hidden;
  margin-bottom: 28px;
}
.req-section .section-header {
  padding: 16px 20px;
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.req-section h3 {
  font-size: 14px;
  font-weight: 500;
  color: var(--text2);
}
table {
  width: 100%;
  border-collapse: collapse;
}
th {
  text-align: left;
  padding: 10px 16px;
  font-size: 11px;
  color: var(--text2);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  background: var(--surface2);
  border-bottom: 1px solid var(--border);
}
td {
  padding: 10px 16px;
  font-size: 13px;
  border-bottom: 1px solid var(--border);
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: rgba(108,140,255,0.04); }
td.mono {
  font-family: var(--font);
  font-size: 12px;
}
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 600;
  font-family: var(--font);
}
.badge.ok { background: rgba(78,205,196,0.15); color: var(--green); }
.badge.error { background: rgba(255,107,107,0.15); color: var(--red); }
.badge.running { background: rgba(255,169,77,0.15); color: var(--orange); }

/* ─── Modal ─── */
.overlay {
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.6);
  z-index: 100;
  justify-content: center;
  align-items: flex-start;
  padding-top: 60px;
  overflow-y: auto;
}
.overlay.open { display: flex; }
.modal {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  width: 90%;
  max-width: 800px;
  margin-bottom: 60px;
}
.modal-header {
  padding: 20px 24px;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.modal-header h2 {
  font-size: 16px;
  font-weight: 600;
}
.modal-header .close-btn {
  background: none;
  border: none;
  color: var(--text2);
  font-size: 20px;
  cursor: pointer;
  padding: 4px 8px;
}
.modal-body { padding: 24px; }

/* waterfall */
.waterfall { position: relative; margin-top: 16px; }
.wf-row {
  display: flex;
  align-items: center;
  margin-bottom: 6px;
  gap: 12px;
}
.wf-row .wf-label {
  flex: 0 0 180px;
  font-family: var(--font);
  font-size: 12px;
  text-align: right;
  color: var(--text);
}
.wf-row .wf-track {
  flex: 1;
  height: 22px;
  position: relative;
}
.wf-row .wf-bar {
  position: absolute;
  height: 100%;
  border-radius: 3px;
  display: flex;
  align-items: center;
  padding-left: 6px;
  font-family: var(--font);
  font-size: 10px;
  font-weight: 600;
  color: #fff;
  white-space: nowrap;
}
.wf-row .wf-time {
  flex: 0 0 80px;
  font-family: var(--font);
  font-size: 12px;
  color: var(--text2);
  text-align: right;
}
.wf-scale {
  display: flex;
  justify-content: space-between;
  font-size: 10px;
  color: var(--text2);
  font-family: var(--font);
  margin-top: 4px;
  padding-left: 192px;
  padding-right: 92px;
}

.colors-list { --c0:#6c8cff; --c1:#4ecdc4; --c2:#ffa94d; --c3:#ff6b6b; --c4:#ffe066; --c5:#a78bfa; --c6:#f472b6; --c7:#38bdf8; }

.empty-state {
  text-align: center;
  padding: 60px 20px;
  color: var(--text2);
}
.empty-state .icon { font-size: 48px; margin-bottom: 12px; opacity: 0.4; }
.empty-state p { font-size: 14px; }

/* ─── Conversation View ─── */
.conv-timeline { display: flex; flex-direction: column; gap: 8px; margin-top: 16px; max-height: 60vh; overflow-y: auto; padding-right: 4px; }
.conv-msg { border-radius: 8px; padding: 10px 14px; font-size: 13px; line-height: 1.6; word-break: break-word; }
.conv-msg.status { background: var(--surface2); color: var(--text2); font-size: 12px; border-left: 3px solid var(--accent); }
.conv-msg.delta-text { background: rgba(108,140,255,0.08); color: var(--text); white-space: pre-wrap; font-family: var(--font); font-size: 12px; border-left: 3px solid var(--accent); }
.conv-msg.delta-reasoning { background: rgba(255,224,102,0.06); color: var(--text2); font-style: italic; white-space: pre-wrap; font-family: var(--font); font-size: 12px; border-left: 3px solid var(--yellow); }
.conv-msg.tool-call { background: var(--surface2); border-left: 3px solid var(--accent2); }
.conv-msg.tool-call .tool-header { font-weight: 600; font-size: 12px; color: var(--accent2); margin-bottom: 6px; display: flex; align-items: center; gap: 6px; }
.conv-msg.tool-call .tool-body { font-family: var(--font); font-size: 11px; color: var(--text2); white-space: pre-wrap; max-height: 200px; overflow-y: auto; }
.conv-msg.tool-call .tool-toggle { cursor: pointer; user-select: none; }
.conv-msg.result { background: rgba(78,205,196,0.1); border-left: 3px solid var(--green); color: var(--green); font-weight: 600; }
.conv-msg.error { background: rgba(255,107,107,0.1); border-left: 3px solid var(--red); color: var(--red); }
.conv-msg.meta { background: var(--surface2); color: var(--text2); font-size: 11px; font-family: var(--font); }
.conv-msg .ts { font-size: 10px; color: var(--text2); opacity: 0.6; margin-top: 4px; }
.conv-btn { background: none; border: 1px solid var(--accent); color: var(--accent); padding: 4px 10px; border-radius: 5px; font-size: 11px; cursor: pointer; font-weight: 600; transition: all .15s; }
.conv-btn:hover { background: var(--accent); color: #fff; }

.filter-row {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}
.filter-btn {
  background: var(--surface2);
  color: var(--text2);
  border: 1px solid var(--border);
  padding: 4px 12px;
  border-radius: 14px;
  font-size: 12px;
  cursor: pointer;
  transition: all .15s;
}
.filter-btn.active, .filter-btn:hover {
  border-color: var(--accent);
  color: var(--accent);
  background: rgba(108,140,255,0.08);
}
</style>
</head>
<body>

<div class="header">
  <h1><span>Workshop</span> 性能监控</h1>
  <div class="controls">
    <span class="auto-tag" id="autoTag">自动刷新 10s</span>
    <select id="hoursSelect">
      <option value="1">最近 1 小时</option>
      <option value="6">最近 6 小时</option>
      <option value="24" selected>最近 24 小时</option>
      <option value="72">最近 3 天</option>
      <option value="168">最近 7 天</option>
    </select>
    <button class="refresh" onclick="refreshAll()">刷新</button>
  </div>
</div>

<div class="container">
  <div id="statsGrid" class="stats-grid"></div>
  <div id="stepSection"></div>

  <div class="req-section">
    <div class="section-header">
      <h3>最近请求</h3>
      <div class="filter-row" id="filterRow"></div>
    </div>
    <div id="reqTable"></div>
  </div>
</div>

<div class="overlay" id="overlay" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <div class="modal-header">
      <h2 id="modalTitle">请求详情</h2>
      <button class="close-btn" onclick="closeModal()">&times;</button>
    </div>
    <div class="modal-body" id="modalBody"></div>
  </div>
</div>

<script>
const BASE = '';
let currentFilter = null;
let autoTimer = null;

const STEP_COLORS = ['#6c8cff','#4ecdc4','#ffa94d','#ff6b6b','#ffe066','#a78bfa','#f472b6','#38bdf8'];
const EP_LABELS = {
  'opencode/generate-preview': 'OpenCode 生成预览',
  'opencode/generate-preview/stream': 'OpenCode 流式生成',
  'opencode/preview-proxy': 'OpenCode 预览代理',
  'generate': 'DashScope 生成',
  'upload': 'OSS 上传',
};
const STEP_LABELS = {
  'request_received': '请求接收',
  'health_check': '健康检查',
  'upstream_generate': '上游生成',
  'build_response': '构造响应',
  'upstream_connect': '上游连接',
  'stream_first_event': '首事件到达',
  'stream_complete': '流式传输完成',
  'llm_connect': 'LLM 连接',
  'llm_first_token': '首 Token',
  'stream_transfer': '流式传输',
  'read_file': '读取文件',
  'oss_upload': 'OSS 上传',
  'proxy_forward': '代理转发',
};

function fmtMs(ms) {
  if (ms == null) return '-';
  if (ms < 1000) return ms.toFixed(0) + 'ms';
  return (ms / 1000).toFixed(2) + 's';
}

function fmtTime(ts) {
  if (!ts) return '-';
  const d = new Date(ts * 1000);
  return d.toLocaleString('zh-CN', {hour12:false, month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit'});
}

function epLabel(ep) { return EP_LABELS[ep] || ep; }
function stepLabel(s) { return STEP_LABELS[s] || s; }

async function fetchJson(url) {
  const r = await fetch(BASE + url);
  return r.json();
}

function getHours() { return parseInt(document.getElementById('hoursSelect').value); }

async function loadStats() {
  const data = await fetchJson(`/api/stats?hours=${getHours()}`);
  const grid = document.getElementById('statsGrid');
  if (!data.endpoints || data.endpoints.length === 0) {
    grid.innerHTML = `<div class="empty-state"><div class="icon">📊</div><p>暂无数据，等待请求进入...</p></div>`;
    return;
  }
  grid.innerHTML = data.endpoints.map(ep => `
    <div class="stat-card" onclick="showStepStats('${ep.endpoint}')">
      <div class="ep-name">${epLabel(ep.endpoint)}</div>
      <div class="metrics">
        <div class="metric"><div class="val">${ep.cnt}</div><div class="label">请求数</div></div>
        <div class="metric"><div class="val">${fmtMs(ep.avg_ms)}</div><div class="label">平均耗时</div></div>
        <div class="metric err"><div class="val">${ep.errors}</div><div class="label">错误</div></div>
      </div>
      <div class="extra">
        <div class="metric"><div class="val">${fmtMs(ep.min_ms)}</div><div class="label">最快</div></div>
        <div class="metric"><div class="val">${fmtMs(ep.max_ms)}</div><div class="label">最慢</div></div>
      </div>
    </div>
  `).join('');
}

async function showStepStats(endpoint) {
  const data = await fetchJson(`/api/step-stats?endpoint=${encodeURIComponent(endpoint)}&hours=${getHours()}`);
  const sec = document.getElementById('stepSection');
  if (!data || data.length === 0) {
    sec.innerHTML = `<div class="step-section"><h3>步骤耗时分布 · <code>${epLabel(endpoint)}</code></h3><p style="color:var(--text2);padding:20px 0">暂无步骤数据</p></div>`;
    return;
  }
  const maxMs = Math.max(...data.map(s => s.max_ms || 0), 1);
  const bars = data.map((s, i) => {
    const avgPct = ((s.avg_ms || 0) / maxMs * 100).toFixed(1);
    const color = STEP_COLORS[i % STEP_COLORS.length];
    return `
      <div class="step-bar-row">
        <div class="step-label">${stepLabel(s.step_name)}</div>
        <div class="bar-track">
          <div class="bar-fill" style="width:${avgPct}%;background:${color}">${fmtMs(s.avg_ms)}</div>
        </div>
        <div class="bar-stats">min ${fmtMs(s.min_ms)} / max ${fmtMs(s.max_ms)}</div>
      </div>`;
  }).join('');
  sec.innerHTML = `<div class="step-section"><h3>步骤耗时分布 · <code>${epLabel(endpoint)}</code></h3>${bars}</div>`;
}

async function loadRequests() {
  const params = new URLSearchParams({limit:'50'});
  if (currentFilter) params.set('endpoint', currentFilter);
  const data = await fetchJson(`/api/requests?${params}`);
  const filterRow = document.getElementById('filterRow');
  const endpoints = [...new Set(data.map(r => r.endpoint))];

  let filters = `<button class="filter-btn ${!currentFilter?'active':''}" onclick="setFilter(null)">全部</button>`;
  endpoints.forEach(ep => {
    filters += `<button class="filter-btn ${currentFilter===ep?'active':''}" onclick="setFilter('${ep}')">${epLabel(ep)}</button>`;
  });
  filterRow.innerHTML = filters;

  if (!data.length) {
    document.getElementById('reqTable').innerHTML = `<div class="empty-state"><p>暂无请求记录</p></div>`;
    return;
  }

  const rows = data.map(r => {
    const isStream = r.endpoint && r.endpoint.includes('stream');
    const convBtn = isStream ? `<button class="conv-btn" onclick="event.stopPropagation();showConversation('${r.request_id}')">对话</button>` : '-';
    return `
    <tr style="cursor:pointer" onclick="showDetail('${r.request_id}')">
      <td class="mono">${r.request_id}</td>
      <td>${epLabel(r.endpoint)}</td>
      <td class="mono">${fmtMs(r.total_ms)}</td>
      <td><span class="badge ${r.status}">${r.status}</span></td>
      <td class="mono">${fmtTime(r.started_at)}</td>
      <td>${convBtn}</td>
    </tr>`;
  }).join('');

  document.getElementById('reqTable').innerHTML = `
    <table>
      <thead><tr><th>请求 ID</th><th>端点</th><th>总耗时</th><th>状态</th><th>时间</th><th>对话</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function setFilter(ep) {
  currentFilter = ep;
  loadRequests();
}

async function showDetail(rid) {
  const data = await fetchJson(`/api/request/${rid}`);
  if (!data || data.error) { alert('未找到'); return; }

  document.getElementById('modalTitle').textContent = `${epLabel(data.endpoint)} · ${data.request_id}`;

  let html = `
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px">
      <div class="metric"><div class="val" style="font-size:18px">${fmtMs(data.total_ms)}</div><div class="label">总耗时</div></div>
      <div class="metric"><div class="val badge ${data.status}" style="font-size:14px">${data.status}</div><div class="label">状态</div></div>
      <div class="metric"><div class="val" style="font-size:14px">${fmtTime(data.started_at)}</div><div class="label">开始时间</div></div>
      <div class="metric"><div class="val" style="font-size:14px">${(data.steps||[]).length}</div><div class="label">步骤数</div></div>
    </div>`;

  const steps = data.steps || [];
  if (steps.length > 0) {
    const totalMs = data.total_ms || Math.max(...steps.map(s => (s.started_at - data.started_at)*1000 + (s.duration_ms||0)));
    const t0 = data.started_at;

    html += `<h3 style="font-size:14px;color:var(--text2);margin-bottom:12px">瀑布图 · Waterfall</h3>`;
    html += `<div class="waterfall">`;
    steps.forEach((s, i) => {
      const offset = Math.max(0, (s.started_at - t0) * 1000);
      const leftPct = totalMs > 0 ? (offset / totalMs * 100) : 0;
      const widthPct = totalMs > 0 ? ((s.duration_ms || 0) / totalMs * 100) : 0;
      const color = STEP_COLORS[i % STEP_COLORS.length];
      html += `
        <div class="wf-row">
          <div class="wf-label">${stepLabel(s.step_name)}</div>
          <div class="wf-track">
            <div class="wf-bar" style="left:${leftPct}%;width:${Math.max(widthPct, 1)}%;background:${color}">
              ${widthPct > 8 ? fmtMs(s.duration_ms) : ''}
            </div>
          </div>
          <div class="wf-time">${fmtMs(s.duration_ms)}</div>
        </div>`;
    });

    const scaleSteps = 5;
    const labels = Array.from({length: scaleSteps + 1}, (_, i) => fmtMs(totalMs / scaleSteps * i));
    html += `<div class="wf-scale">${labels.map(l => `<span>${l}</span>`).join('')}</div>`;
    html += `</div>`;

    html += `<table style="margin-top:20px">
      <thead><tr><th>#</th><th>步骤</th><th>耗时</th><th>状态</th><th>元信息</th></tr></thead><tbody>`;
    steps.forEach((s, i) => {
      let metaStr = '';
      try { const m = JSON.parse(s.meta || '{}'); metaStr = Object.keys(m).length ? JSON.stringify(m) : ''; } catch(e){}
      html += `<tr>
        <td>${i+1}</td>
        <td class="mono">${stepLabel(s.step_name)}</td>
        <td class="mono">${fmtMs(s.duration_ms)}</td>
        <td><span class="badge ${s.status}">${s.status}</span></td>
        <td class="mono" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${metaStr.replace(/"/g,'&quot;')}">${metaStr || '-'}</td>
      </tr>`;
    });
    html += `</tbody></table>`;
  }

  if (data.endpoint && data.endpoint.includes('stream')) {
    html += `<div style="margin-top:20px;padding-top:16px;border-top:1px solid var(--border)">
      <button class="conv-btn" style="font-size:13px;padding:8px 16px" onclick="showConversation('${data.request_id}')">查看 OpenCode 对话日志</button>
    </div>`;
  }

  document.getElementById('modalBody').innerHTML = html;
  document.getElementById('overlay').classList.add('open');
}

async function showConversation(rid) {
  document.getElementById('modalTitle').textContent = `OpenCode 对话日志 · ${rid}`;
  document.getElementById('modalBody').innerHTML = '<p style="color:var(--text2)">加载中...</p>';
  document.getElementById('overlay').classList.add('open');

  let data;
  try { data = await fetchJson(`/api/conversation/${rid}`); } catch(e) { data = null; }

  if (!data || data.error || !data.events || data.events.length === 0) {
    document.getElementById('modalBody').innerHTML = '<div class="empty-state"><p>暂无对话事件</p></div>';
    return;
  }

  let textAcc = '';
  let reasonAcc = '';
  const merged = [];

  function flushText() {
    if (textAcc) { merged.push({type:'delta-text', content: textAcc}); textAcc = ''; }
  }
  function flushReason() {
    if (reasonAcc) { merged.push({type:'delta-reasoning', content: reasonAcc}); reasonAcc = ''; }
  }

  for (const ev of data.events) {
    const et = ev.event_type;
    let payload;
    try { payload = typeof ev.payload === 'string' ? JSON.parse(ev.payload) : ev.payload; } catch(e) { payload = {}; }

    if (et === 'delta') {
      const pt = payload.partType || 'text';
      if (pt === 'reasoning') {
        flushText();
        reasonAcc += payload.content || '';
      } else {
        flushReason();
        textAcc += payload.content || '';
      }
    } else {
      flushText();
      flushReason();
      merged.push({type: et, payload, summary: ev.summary, ts: ev.ts});
    }
  }
  flushText();
  flushReason();

  const esc = s => (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

  let html = '<div class="conv-timeline">';
  for (const m of merged) {
    if (m.type === 'delta-text') {
      html += `<div class="conv-msg delta-text">${esc(m.content)}</div>`;
    } else if (m.type === 'delta-reasoning') {
      html += `<div class="conv-msg delta-reasoning"><strong>Thinking</strong>\n${esc(m.content)}</div>`;
    } else if (m.type === 'status') {
      const stage = m.payload?.stage || '';
      const content = m.payload?.content || m.summary || '';
      html += `<div class="conv-msg status"><strong>${esc(stage)}</strong> ${esc(content)}</div>`;
    } else if (m.type === 'tool') {
      const p = m.payload || {};
      const statusIcon = p.status === 'error' ? '⚠' : p.status === 'completed' ? '✓' : '⏳';
      const statusCls = p.status === 'error' ? 'color:var(--red)' : p.status === 'completed' ? 'color:var(--green)' : '';
      const bodyParts = [];
      if (p.input) bodyParts.push('Input:\n' + p.input);
      if (p.output) bodyParts.push('Output:\n' + p.output);
      if (p.error) bodyParts.push('Error:\n' + p.error);
      html += `<div class="conv-msg tool-call">
        <div class="tool-header"><span style="${statusCls}">${statusIcon}</span> ${esc(p.title || p.tool || 'tool')} <span class="badge ${p.status || ''}">${p.status || ''}</span></div>
        ${bodyParts.length ? `<div class="tool-body">${esc(bodyParts.join('\n\n'))}</div>` : ''}
      </div>`;
    } else if (m.type === 'result') {
      const url = m.payload?.url || m.payload?.urlPath || m.summary || '';
      html += `<div class="conv-msg result">✓ 预览就绪: ${esc(url)}</div>`;
    } else if (m.type === 'error') {
      html += `<div class="conv-msg error">✕ ${esc(m.payload?.content || m.summary || 'error')}</div>`;
    } else if (m.type === 'meta') {
      html += `<div class="conv-msg meta">Session: ${esc(m.payload?.opencodeSessionId || '-')} · Workspace: ${esc(m.payload?.workspacePath || '-')}</div>`;
    } else if (m.type === 'todo') {
      const todos = m.payload?.todos || [];
      const items = todos.map(t => `${t.status === 'completed' ? '✓' : t.status === 'in_progress' ? '⏳' : '○'} ${t.content || t.id}`).join('\n');
      html += `<div class="conv-msg status"><strong>待办列表</strong>\n${esc(items)}</div>`;
    } else {
      html += `<div class="conv-msg meta">${esc(m.type)}: ${esc(m.summary || JSON.stringify(m.payload))}</div>`;
    }
  }
  html += '</div>';
  html += `<p style="margin-top:12px;font-size:11px;color:var(--text2)">共 ${data.events.length} 个事件，合并后 ${merged.length} 条消息</p>`;
  document.getElementById('modalBody').innerHTML = html;
}

function closeModal() {
  document.getElementById('overlay').classList.remove('open');
}

async function refreshAll() {
  await Promise.all([loadStats(), loadRequests()]);
}

document.getElementById('hoursSelect').addEventListener('change', refreshAll);
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

refreshAll();
autoTimer = setInterval(refreshAll, 10000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=11000)
