import os
import glob
import re
import sqlite3
from collections import defaultdict
from contextlib import closing
import asyncio
import json
import socket
import urllib.request
import urllib.error
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse

from env_loader import load_crawl_env
from tz_display import db_value_to_shanghai_display, format_shanghai_local, naive_utc_from_timestamp
from news_agents import hacker_news_P_T, HN_GRAVITY

load_crawl_env()


DB_PATH = os.getenv("AI_NEWS_DB_PATH", "/app/db/ai_news.db")
LOG_PATH = os.getenv("CRAWL_LOG_PATH", "/app/logs/crawl_log.txt")
WORKER_HEALTH_URL = os.getenv("WORKER_HEALTH_URL", "http://crawler:6600/health")
PUSH_API_BASE = os.getenv("PUSH_API_BASE", "http://api:8000").rstrip("/")
PUSH_API_TIMEOUT_SECONDS = float(os.getenv("PUSH_API_TIMEOUT_SECONDS", "120"))

LOG_ROOT = os.path.realpath(os.path.dirname(os.path.abspath(LOG_PATH)))
LOG_BASENAME = os.path.basename(LOG_PATH)
if not os.path.isdir(LOG_ROOT):
    _local_log_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    if os.path.isdir(_local_log_root):
        LOG_ROOT = os.path.realpath(_local_log_root)
        LOG_PATH = os.path.join(LOG_ROOT, LOG_BASENAME)

app = FastAPI(title="Crawl Monitor")


def _ensure_source_key_column():
    """旧库补列：与 db.init_db 迁移保持一致，便于仅启动监控页时也可读写的库结构一致。"""
    if not os.path.exists(DB_PATH):
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.execute("PRAGMA table_info(news_articles)")
        cols = {r[1] for r in cur.fetchall()}
        if cols and "source_key" not in cols:
            conn.execute(
                "ALTER TABLE news_articles ADD COLUMN source_key TEXT DEFAULT ''"
            )
            conn.commit()


def _query(sql: str, params=()):
    if not os.path.exists(DB_PATH):
        return []
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        with closing(conn.cursor()) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def _normalize_user_role(role: str | None, username: str | None = None) -> str:
    normalized = str(role or "").strip().lower()
    if normalized == "admin":
        return "admin"
    if str(username or "").strip().lower() == "admin":
        return "admin"
    return "user"


def _get_session_user_by_token(token: str | None) -> dict | None:
    normalized_token = str(token or "").strip()
    if not normalized_token or not os.path.exists(DB_PATH):
        return None
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT u.id, u.username, u.display_name, u.role, s.token, s.updated_at
                FROM app_sessions AS s
                JOIN app_users AS u
                  ON u.username = s.username
                WHERE s.token = ?
                LIMIT 1
                """,
                (normalized_token,),
            ).fetchone()
    except sqlite3.Error:
        return None
    if not row:
        return None

    user = dict(row)
    user["role"] = _normalize_user_role(user.get("role"), user.get("username"))
    user["isAdmin"] = user["role"] == "admin"
    user["token"] = normalized_token
    return user


def _extract_request_token(request: Request, token: str | None = None) -> str:
    header = str(request.headers.get("authorization") or "").strip()
    if header.lower().startswith("bearer "):
        bearer_token = header.split(" ", 1)[1].strip()
        if bearer_token:
            return bearer_token
    return str(token or "").strip()


async def _require_admin_request(
    request: Request,
    token: str | None = Query(default=None),
) -> dict:
    resolved_token = _extract_request_token(request, token)
    user = _get_session_user_by_token(resolved_token)
    if not user:
        raise HTTPException(status_code=401, detail="admin authentication required")
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="admin access required")
    return user


def _looks_like_utf8_mojibake(text: str) -> bool:
    if not text:
        return False
    if any(0x80 <= ord(ch) <= 0x9F for ch in text):
        return True
    return any(marker in text for marker in ("Ã", "Â", "â", "ð", "è", "æ", "å", "ç", "ï", "ô"))


def _repair_utf8_mojibake(value):
    text = str(value or "")
    if not _looks_like_utf8_mojibake(text):
        return text

    repaired = text
    for _ in range(2):
        try:
            candidate = repaired.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            break
        if candidate == repaired:
            break
        repaired = candidate
    return repaired


def _repair_utf8_mojibake_by_line(text: str) -> str:
    lines = str(text or "").splitlines()
    return "\n".join(_repair_utf8_mojibake(line) for line in lines)


def _proxy_push_post(
    path: str,
    payload: dict,
    authorization: str | None = None,
) -> dict:
    url = f"{PUSH_API_BASE}{path}"
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    normalized_authorization = str(authorization or "").strip()
    if normalized_authorization.lower().startswith("bearer "):
        headers["Authorization"] = normalized_authorization
    req = urllib.request.Request(
        url=url,
        method="POST",
        data=data,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=PUSH_API_TIMEOUT_SECONDS) as resp:
            raw = resp.read()
            text = raw.decode("utf-8", errors="replace") if raw else "{}"
            if not text.strip():
                return {}
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {"raw": parsed}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        detail = body or f"push api http error: {exc.code}"
        if body:
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                detail = (
                    parsed.get("detail")
                    or parsed.get("error")
                    or parsed.get("message")
                    or detail
                )
        raise HTTPException(status_code=exc.code, detail=detail) from exc
    except (TimeoutError, socket.timeout) as exc:
        raise HTTPException(
            status_code=504,
            detail=f"push api timeout after {PUSH_API_TIMEOUT_SECONDS:g}s",
        ) from exc
    except urllib.error.URLError as exc:
        raise HTTPException(status_code=502, detail=f"push api unreachable: {exc}") from exc


def _proxy_api_delete(
    path: str,
    authorization: str | None = None,
) -> dict:
    url = f"{PUSH_API_BASE}{path}"
    headers = {}
    normalized_authorization = str(authorization or "").strip()
    if normalized_authorization.lower().startswith("bearer "):
        headers["Authorization"] = normalized_authorization
    req = urllib.request.Request(
        url=url,
        method="DELETE",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=PUSH_API_TIMEOUT_SECONDS) as resp:
            raw = resp.read()
            text = raw.decode("utf-8", errors="replace") if raw else "{}"
            if not text.strip():
                return {}
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {"raw": parsed}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        detail = body or f"api http error: {exc.code}"
        if body:
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                detail = (
                    parsed.get("detail")
                    or parsed.get("error")
                    or parsed.get("message")
                    or detail
                )
        raise HTTPException(status_code=exc.code, detail=detail) from exc
    except (TimeoutError, socket.timeout) as exc:
        raise HTTPException(
            status_code=504,
            detail=f"api timeout after {PUSH_API_TIMEOUT_SECONDS:g}s",
        ) from exc
    except urllib.error.URLError as exc:
        raise HTTPException(status_code=502, detail=f"api unreachable: {exc}") from exc


def _resolve_log_key(key: str | None) -> str | None:
    if not key or key == "current":
        p = os.path.realpath(os.path.abspath(LOG_PATH))
        return p if os.path.isfile(p) else None
    key = key.replace("\\", "/").strip()
    if ".." in key or key.startswith("/"):
        return None
    full = os.path.realpath(os.path.join(LOG_ROOT, key))
    if not full.startswith(LOG_ROOT + os.sep):
        return None
    bn = os.path.basename(full)
    if bn != LOG_BASENAME and not (bn.startswith("crawl_log_") and bn.endswith(".txt")):
        return None
    return full if os.path.isfile(full) else None


def _list_log_sessions():
    archives = sorted(
        glob.glob(os.path.join(LOG_ROOT, "archive", "crawl_log_*.txt")),
        key=os.path.getmtime,
    )
    n_arch = len(archives)
    out = []
    current_path = os.path.realpath(os.path.abspath(LOG_PATH))
    if os.path.isfile(current_path):
        out.append(
            {
                "key": "current",
                "label": f"当前会话（第 {n_arch + 1} 次）",
                "is_current": True,
                "path": current_path,
            }
        )
    for idx, p in enumerate(reversed(archives)):
        run_no = n_arch - idx
        rel = os.path.relpath(p, LOG_ROOT).replace("\\", "/")
        mtime = format_shanghai_local(
            naive_utc_from_timestamp(os.path.getmtime(p))
        )[:16]
        out.append(
            {
                "key": rel,
                "label": f"第 {run_no} 次 · 归档 {mtime}",
                "is_current": False,
                "size": os.path.getsize(p),
                "path": p,
            }
        )
    return out


def _read_log_text(path: str, max_bytes: int = 2_000_000) -> str:
    if not path or not os.path.isfile(path):
        return ""
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        if size > max_bytes:
            f.seek(-max_bytes, os.SEEK_END)
        raw = f.read()
    return raw.decode("utf-8", errors="replace")


_GEN_IMG_RE = re.compile(r"文生图兜底完成:\s*(\d+)\s*/\s*(\d+)")


def _find_generated_images_for_batch(lines: list[str], batch_id: str | None) -> int | None:
    batch = str(batch_id or "").strip()
    if not batch:
        return None
    anchor = None
    for idx, line in enumerate(lines):
        if batch in line and "已存入数据库" in line:
            anchor = idx
    if anchor is None:
        for idx, line in enumerate(lines):
            if batch in line:
                anchor = idx
    if anchor is None:
        return None

    start = max(0, anchor - 800)
    end = min(len(lines), anchor + 80)
    before = []
    after = []
    for idx in range(start, end):
        m = _GEN_IMG_RE.search(lines[idx])
        if not m:
            continue
        count = int(m.group(1))
        if idx <= anchor:
            before.append((idx, count))
        else:
            after.append((idx, count))
    if before:
        return before[-1][1]
    if after:
        return after[0][1]
    return None


def _extract_run_log_metrics(text: str, biz_batch: str | None, personal_batch: str | None) -> dict:
    lines = text.splitlines()
    biz_generated = _find_generated_images_for_batch(lines, biz_batch)
    personal_generated = _find_generated_images_for_batch(lines, personal_batch)
    return {
        "biz_generated_images": biz_generated,
        "personal_generated_images": personal_generated,
        "generated_images": int(biz_generated or 0) + int(personal_generated or 0),
    }


_GEN_IMG_RE = re.compile(r"文生图兜底完成[:：]?\s*(\d+)\s*/\s*(\d+)")


def _find_generated_images_for_batch(lines: list[str], batch_id: str | None) -> int | None:
    batch = str(batch_id or "").strip()
    if not batch:
        return None
    anchor = None
    for idx, line in enumerate(lines):
        if batch in line and "已存入数据库" in line:
            anchor = idx
    if anchor is None:
        for idx, line in enumerate(lines):
            if batch in line:
                anchor = idx
    if anchor is None:
        return None

    start = max(0, anchor - 800)
    end = min(len(lines), anchor + 80)
    before = []
    after = []
    for idx in range(start, end):
        match = _GEN_IMG_RE.search(lines[idx])
        if not match:
            continue
        count = int(match.group(1))
        if idx <= anchor:
            before.append((idx, count))
        else:
            after.append((idx, count))
    if before:
        return before[-1][1]
    if after:
        return after[0][1]
    return None


def _list_log_sessions():
    archives = sorted(
        glob.glob(os.path.join(LOG_ROOT, "archive", "crawl_log_*.txt")),
        key=os.path.getmtime,
    )
    n_arch = len(archives)
    out = []
    current_path = os.path.realpath(os.path.abspath(LOG_PATH))
    if os.path.isfile(current_path):
        out.append(
            {
                "key": "current",
                "label": f"当前会话（第 {n_arch + 1} 次）",
                "is_current": True,
                "path": current_path,
            }
        )
    for idx, path in enumerate(reversed(archives)):
        run_no = n_arch - idx
        rel = os.path.relpath(path, LOG_ROOT).replace("\\", "/")
        mtime = format_shanghai_local(
            naive_utc_from_timestamp(os.path.getmtime(path))
        )[:16]
        out.append(
            {
                "key": rel,
                "label": f"第 {run_no} 次 · 归档 {mtime}",
                "is_current": False,
                "size": os.path.getsize(path),
                "path": path,
            }
        )
    return out


def _match_log_session_for_run(biz_batch: str | None, personal_batch: str | None) -> dict:
    needles = [s for s in [str(biz_batch or "").strip(), str(personal_batch or "").strip()] if s]
    fallback = {
        "matched": False,
        "log_key": "",
        "biz_generated_images": None,
        "personal_generated_images": None,
        "generated_images": 0,
    }
    if not needles:
        return fallback

    for session in _list_log_sessions():
        path = session.get("path") or _resolve_log_key(session.get("key"))
        text = _read_log_text(path or "")
        if not text:
            continue
        if any(needle in text for needle in needles):
            return {
                "matched": True,
                "log_key": session["key"],
                **_extract_run_log_metrics(text, biz_batch, personal_batch),
            }
    return fallback


def _runs_from_batch_rows(rows: list) -> list:
    """将 biz_* / personal_* 成对合并为一次「双榜」爬取轮次。"""
    by_suffix = defaultdict(
        lambda: {
            "biz_batch": None,
            "personal_batch": None,
            "biz_count": 0,
            "personal_count": 0,
            "biz_updated_at": None,
            "personal_updated_at": None,
        }
    )
    for r in rows:
        d = dict(r)
        bid = str(d.get("id") or "")
        cnt = int(d.get("cnt") or 0)
        ua = d.get("updated_at")
        if bid.startswith("biz_"):
            suf = bid[4:]
            slot = by_suffix[suf]
            slot["biz_batch"] = bid
            slot["biz_count"] = cnt
            slot["biz_updated_at"] = ua
        elif bid.startswith("personal_"):
            suf = bid[9:]
            slot = by_suffix[suf]
            slot["personal_batch"] = bid
            slot["personal_count"] = cnt
            slot["personal_updated_at"] = ua

    runs = []
    for suffix, slot in by_suffix.items():
        ts = slot["biz_updated_at"] or slot["personal_updated_at"]
        runs.append(
            {
                "suffix": suffix,
                "biz_batch": slot["biz_batch"],
                "personal_batch": slot["personal_batch"],
                "biz_count": slot["biz_count"],
                "personal_count": slot["personal_count"],
                "updated_at": db_value_to_shanghai_display(ts) if ts is not None else "",
            }
        )
    runs.sort(
        key=lambda x: (x.get("updated_at") or ""),
        reverse=True,
    )

    paired = set()
    for slot in by_suffix.values():
        if slot["biz_batch"]:
            paired.add(slot["biz_batch"])
        if slot["personal_batch"]:
            paired.add(slot["personal_batch"])
    for r in rows:
        bid = str(dict(r).get("id") or "")
        if not bid or bid in paired:
            continue
        if bid.startswith("biz_") or bid.startswith("personal_"):
            continue
        d = dict(r)
        runs.append(
            {
                "suffix": bid,
                "biz_batch": bid,
                "personal_batch": None,
                "biz_count": int(d.get("cnt") or 0),
                "personal_count": 0,
                "updated_at": db_value_to_shanghai_display(d.get("updated_at"))
                if d.get("updated_at") is not None
                else "",
            }
        )
    runs.sort(key=lambda x: (x.get("updated_at") or ""), reverse=True)
    return runs


@app.get("/api/batches")
def batches_list(_admin_user: dict = Depends(_require_admin_request)):
    rows = _query(
        """
        SELECT crawl_batch AS id, COUNT(*) AS cnt, MAX(updated_at) AS updated_at
        FROM news_articles
        GROUP BY crawl_batch
        ORDER BY updated_at DESC
        """
    )
    out = []
    for r in rows:
        d = dict(r)
        d["updated_at"] = db_value_to_shanghai_display(d.get("updated_at"))
        out.append(d)
    runs = _runs_from_batch_rows(rows)
    return {"batches": out, "runs": runs}


@app.get("/api/batch_view")
def batch_view(
    batch: str | None = None,
    _admin_user: dict = Depends(_require_admin_request),
):
    _ensure_source_key_column()
    effective = (batch or "").strip()
    if not effective:
        row = _query(
            "SELECT crawl_batch FROM news_articles ORDER BY updated_at DESC LIMIT 1"
        )
        effective = row[0]["crawl_batch"] if row else ""
    if not effective:
        return {"batch": "", "source_distribution": [], "articles": []}
    source_dist = _query(
        """
        SELECT source, COUNT(*) AS count
        FROM news_articles
        WHERE crawl_batch = ?
        GROUP BY source
        ORDER BY count DESC
        """,
        (effective,),
    )
    for item in source_dist:
        item["source"] = _repair_utf8_mojibake(item.get("source"))
    articles = _query(
        """
        SELECT id, title, url, source, source_key, total_score, spread_heat, published_at, updated_at
        FROM news_articles
        WHERE crawl_batch = ?
        ORDER BY total_score DESC, updated_at DESC
        """,
        (effective,),
    )
    arts = []
    for a in articles:
        d = dict(a)
        d["title"] = _repair_utf8_mojibake(d.get("title"))
        d["source"] = _repair_utf8_mojibake(d.get("source"))
        d["updated_at"] = db_value_to_shanghai_display(d.get("updated_at"))
        p, t = hacker_news_P_T(d)
        d["hn_p"] = round(p, 2)
        d["hn_t"] = round(t, 2)
        d["hn_g"] = HN_GRAVITY
        arts.append(d)
    return {
        "batch": effective,
        "source_distribution": source_dist,
        "articles": arts,
    }


@app.get("/api/logs/sessions")
def log_sessions(_admin_user: dict = Depends(_require_admin_request)):
    out = []
    for session in _list_log_sessions():
        item = {
            "key": session["key"],
            "label": session["label"],
            "is_current": session["is_current"],
        }
        if "size" in session:
            item["size"] = session["size"]
        out.append(item)
    return {"sessions": out}


@app.get("/api/runs/log_match")
def run_log_match(
    suffix: str = Query(""),
    biz_batch: str = Query(""),
    personal_batch: str = Query(""),
    _admin_user: dict = Depends(_require_admin_request),
):
    matched = _match_log_session_for_run(biz_batch, personal_batch)
    return {
        "suffix": suffix,
        "biz_batch": biz_batch,
        "personal_batch": personal_batch,
        **matched,
    }


@app.post("/api/push/selected")
async def push_selected_issue(
    request: Request,
    admin_user: dict = Depends(_require_admin_request),
):
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid payload")
    raw_ids = payload.get("articleIds")
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="articleIds is required")

    article_ids = []
    seen = set()
    for value in raw_ids:
        try:
            article_id = int(value)
        except (TypeError, ValueError):
            continue
        if article_id <= 0 or article_id in seen:
            continue
        seen.add(article_id)
        article_ids.append(article_id)

    if len(article_ids) != 3:
        raise HTTPException(status_code=400, detail="articleIds must contain exactly 3 unique ids")

    req_payload = {
        "articleIds": article_ids,
        "title": str(payload.get("title", "") or "").strip(),
        "body": str(payload.get("body", "") or "").strip(),
    }
    authorization = request.headers.get("authorization") or f"Bearer {admin_user.get('token', '')}"
    return _proxy_push_post("/api/push/selected", req_payload, authorization=authorization)


@app.post("/api/push/selected/wechat-publish")
async def publish_selected_issue_to_wechat(
    request: Request,
    admin_user: dict = Depends(_require_admin_request),
):
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid payload")
    raw_ids = payload.get("articleIds")
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="articleIds is required")

    article_ids = []
    seen = set()
    for value in raw_ids:
        try:
            article_id = int(value)
        except (TypeError, ValueError):
            continue
        if article_id <= 0 or article_id in seen:
            continue
        seen.add(article_id)
        article_ids.append(article_id)

    if len(article_ids) != 3:
        raise HTTPException(status_code=400, detail="articleIds must contain exactly 3 unique ids")

    authorization = request.headers.get("authorization") or f"Bearer {admin_user.get('token', '')}"
    return _proxy_push_post(
        "/api/push/selected/wechat-publish",
        {"articleIds": article_ids},
        authorization=authorization,
    )


@app.post("/api/push/selected/email-send")
async def send_selected_issue_by_email(
    request: Request,
    admin_user: dict = Depends(_require_admin_request),
):
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid payload")
    raw_ids = payload.get("articleIds")
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="articleIds is required")

    article_ids = []
    seen = set()
    for value in raw_ids:
        try:
            article_id = int(value)
        except (TypeError, ValueError):
            continue
        if article_id <= 0 or article_id in seen:
            continue
        seen.add(article_id)
        article_ids.append(article_id)

    if len(article_ids) != 3:
        raise HTTPException(status_code=400, detail="articleIds must contain exactly 3 unique ids")

    authorization = request.headers.get("authorization") or f"Bearer {admin_user.get('token', '')}"
    return _proxy_push_post(
        "/api/push/selected/email-send",
        {"articleIds": article_ids},
        authorization=authorization,
    )


@app.delete("/api/auth/sessions/current")
async def logout_current_monitor_session(request: Request):
    authorization = request.headers.get("authorization") or ""
    normalized_authorization = str(authorization).strip()
    if normalized_authorization.lower().startswith("bearer "):
        try:
            _proxy_api_delete(
                "/api/auth/sessions/current",
                authorization=normalized_authorization,
            )
        except HTTPException as exc:
            if exc.status_code not in (401, 403):
                raise
    return {"success": True}


@app.get("/api/logs/content")
def logs_content(
    key: str = Query("current"),
    max_bytes: int = Query(2_000_000, ge=1_000, le=8_000_000),
    _admin_user: dict = Depends(_require_admin_request),
):
    path = _resolve_log_key(key)
    if not path:
        return JSONResponse({"error": "not found", "text": ""}, status_code=404)
    size = os.path.getsize(path)
    truncated = False
    with open(path, "rb") as f:
        if size > max_bytes:
            f.seek(-max_bytes, os.SEEK_END)
            truncated = True
            raw = f.read()
        else:
            raw = f.read()
    raw = _repair_utf8_mojibake_by_line(raw.decode("utf-8", errors="replace")).encode("utf-8")
    text = raw.decode("utf-8", errors="replace")
    if truncated:
        text = f"…（仅展示末尾约 {max_bytes // 1_000_000} MB）…\n{text}"
    return {"text": _repair_utf8_mojibake_by_line(text), "truncated": truncated, "size": size}


@app.get("/api/status")
def status(_admin_user: dict = Depends(_require_admin_request)):
    latest_batch = _query(
        """
        SELECT crawl_batch, COUNT(*) AS count, MAX(updated_at) AS updated_at
        FROM news_articles
        GROUP BY crawl_batch
        ORDER BY updated_at DESC
        LIMIT 1
        """
    )

    worker_online = False
    try:
        with urllib.request.urlopen(WORKER_HEALTH_URL, timeout=1.5) as resp:
            worker_online = resp.status == 200
    except (urllib.error.URLError, TimeoutError, ValueError):
        worker_online = False

    lb = dict(latest_batch[0]) if latest_batch else {}
    if lb.get("updated_at") is not None:
        lb["updated_at"] = db_value_to_shanghai_display(lb.get("updated_at"))

    return {
        "db_exists": os.path.exists(DB_PATH),
        "now": format_shanghai_local(),
        "timezone": "Asia/Shanghai (UTC+8)",
        "worker_online": worker_online,
        "latest_batch": lb,
    }


@app.get("/api/logs/stream")
async def logs_stream(
    key: str = Query("current"),
    _admin_user: dict = Depends(_require_admin_request),
):
    path = _resolve_log_key(key)
    current_real = os.path.realpath(os.path.abspath(LOG_PATH))
    is_live = path == current_real if path else False

    async def event_generator():
        if not path or not os.path.exists(path):
            payload = {"type": "init", "chunk": "(暂无日志文件)"}
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            return

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()[-80:]
        if lines:
            payload = {"type": "init", "chunk": "".join(lines)}
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
        else:
            payload = {"type": "init", "chunk": "(暂无日志)"}
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        if not is_live:
            return

        pos = os.path.getsize(path)
        while True:
            try:
                size = os.path.getsize(path)
                if size < pos:
                    pos = 0
                if size > pos:
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        f.seek(pos)
                        chunk = f.read()
                    pos = size
                    if chunk:
                        payload = {"type": "append", "chunk": chunk}
                        yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/", response_class=HTMLResponse)
def page():
    html = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>爬虫进度看板</title>
  <style>
    :root {
      --bg: #f7f9fc;
      --panel: #ffffff;
      --panel-border: #d9e2f2;
      --text: #152033;
      --muted: #5f6f89;
      --accent: #2b65d9;
      --accent-soft: #eef4ff;
      --accent-border: #bfd0f6;
      --line: #e6edf8;
      --success: #1d8f5a;
      --shadow: 0 14px 36px rgba(24, 42, 77, 0.08);
    }
    body {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      background:
        radial-gradient(circle at top right, rgba(43, 101, 217, 0.08), transparent 24%),
        linear-gradient(180deg, #fbfcff 0%, var(--bg) 100%);
      color: var(--text);
      margin: 0;
    }
    .wrap { max-width: 1200px; margin: 24px auto; padding: 0 16px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .card {
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 16px;
      padding: 14px;
      box-shadow: var(--shadow);
    }
    h1 { margin: 0 0 12px; font-size: 22px; }
    h3 { margin: 0 0 10px; font-size: 14px; color: var(--accent); }
    .kv { line-height: 1.8; font-size: 13px; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    td, th { border-bottom: 1px solid var(--line); padding: 6px; text-align: left; }
    pre {
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 12px;
      max-height: 420px;
      overflow: auto;
      background: #fcfdff;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      color: var(--text);
    }
    .muted { color: var(--muted); font-size: 12px; }
    .log-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; margin-bottom: 10px; }
    .log-head h3 { margin: 0; flex: 1; min-width: 0; }
    .toolbar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-bottom: 10px; }
    .toolbar label { font-size: 12px; color: var(--muted); }
    select.toolbar-select {
      font: inherit; font-size: 12px; padding: 6px 10px; border-radius: 8px;
      border: 1px solid var(--accent-border); background: var(--panel); color: var(--text); max-width: min(420px, 100%);
    }
    .btn-copy {
      font: inherit; font-size: 12px; cursor: pointer; padding: 6px 12px; border-radius: 8px;
      border: 1px solid var(--accent-border); background: var(--accent-soft); color: var(--accent);
    }
    .btn-copy:hover { background: #dfeafe; border-color: var(--accent); }
    .btn-copy:active { transform: scale(0.98); }
    table a.title-link { color: var(--accent); text-decoration: none; word-break: break-word; }
    table a.title-link:hover { text-decoration: underline; color: #1848a8; }
    td.num, th.num { text-align: right; white-space: nowrap; }
    .board-grid { display: grid; grid-template-columns: 1fr; gap: 16px; align-items: start; }
    .board-grid > div { min-width: 0; }
    h4.board-head { margin: 12px 0 8px; font-size: 13px; color: var(--muted); font-weight: 600; }
    h4.board-head:first-of-type { margin-top: 0; }
    .max-cell { font-weight: 700; color: var(--success); letter-spacing: 0.02em; }
    .push-bar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin: 8px 0 12px; }
    .btn-action {
      font: inherit; font-size: 12px; cursor: pointer; padding: 6px 12px; border-radius: 8px;
      border: 1px solid var(--accent-border); background: var(--accent-soft); color: var(--accent);
    }
    .btn-action:hover { background: #dfeafe; border-color: var(--accent); }
    .btn-action:disabled { opacity: 0.55; cursor: not-allowed; }
    th.pick-col, td.pick-col { width: 40px; text-align: center; }
    table.board-table { table-layout: fixed; }
    table.board-table th,
    table.board-table td {
      vertical-align: top;
      padding: 5px 8px;
      overflow: hidden;
    }
    table.board-table th.source-col,
    table.board-table td.source-col {
      width: 156px;
      max-width: 156px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    table.board-table th.title-col,
    table.board-table td.title-col {
      width: auto;
      padding-left: 12px;
    }
    table.board-table th.num,
    table.board-table td.num { width: 52px; }
    table.board-table a.title-link,
    table.board-table .title-text {
      display: block;
      line-height: 1.5;
      white-space: normal;
      word-break: break-word;
      overflow-wrap: anywhere;
      writing-mode: horizontal-tb;
    }
    .page-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }
    .page-head h1 { margin: 0; }
    .page-head-actions {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>爬虫进度看板 <span class="muted">(状态与数据 5s 刷新)</span></h1>
    <div class="page-head-actions" style="justify-content:flex-end; margin: 0 0 12px;">
      <button type="button" class="btn-copy" id="logoutBtn">退出登录</button>
    </div>
    <div class="row">
      <div class="card">
        <h3>运行状态</h3>
        <div id="status" class="kv"></div>
      </div>
      <div class="card">
        <div class="toolbar">
          <h3 style="margin:0;">批次来源分布</h3>
        </div>
        <div class="toolbar">
          <label for="runSelect">双榜轮次</label>
          <select id="runSelect" class="toolbar-select" title="企业榜与个人榜同一轮次"></select>
        </div>
        <p class="muted" id="runSummary" style="margin:0 0 8px;"></p>
        <h4 class="board-head">企业榜 · 来源分布</h4>
        <table id="distBiz"></table>
        <h4 class="board-head">个人榜 · 来源分布</h4>
        <table id="distPersonal"></table>
      </div>
    </div>
    <div class="row" style="margin-top: 16px; grid-template-columns: 1fr;">
      <div class="card">
        <div class="toolbar">
          <h3 style="margin:0;">爬取文章列表</h3>
        </div>
        <p class="muted" id="batchHint" style="margin:0 0 8px;">双榜数据由爬虫分别写入企业批次（biz_*）与个人批次（personal_*）。仅「AI资讯速览」条目的 P、T、分数显示为 MAX（榜单优先级最高）；其余来源为实时公式值。资讯速览的标题链接指向 RSS 正文中 Sources 的第一个外链。</p>
        <div class="push-bar">
          <button type="button" class="btn-action" id="wechatPublishBtn" disabled>Publish to WeChat</button>
          <button type="button" class="btn-action" id="emailSendBtn" disabled>Send Email</button>
          <button type="button" class="btn-action" id="pushSelectedBtn" disabled>推送所选 3 条</button>
          <span class="muted" id="pushPickHint">已选 0 / 3</span>
          <span class="muted" id="pushStatus"></span>
          <span class="muted" id="emailStatus"></span>
          <span class="muted" id="wechatStatus"></span>
        </div>
        <div class="board-grid">
          <div>
            <h4 class="board-head">企业榜</h4>
            <table id="titlesBiz" class="board-table"></table>
          </div>
          <div>
            <h4 class="board-head">个人榜</h4>
            <table id="titlesPersonal" class="board-table"></table>
          </div>
        </div>
      </div>
      <div class="card">
        <div class="log-head">
          <h3>爬虫日志</h3>
          <button type="button" class="btn-copy" id="copyLogs" title="复制当前显示的日志">复制</button>
        </div>
        <div class="toolbar">
          <label for="logSessionSelect">日志会话</label>
          <select id="logSessionSelect" class="toolbar-select" title="当前 run 或历次归档"></select>
        </div>
        <p class="muted" id="logHint" style="margin:0 0 8px;"></p>
        <pre id="logs"></pre>
      </div>
    </div>
  </div>
  <script>
    function escHtml(t) {
      return String(t ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }
    function safeHttpUrl(u) {
      const s = String(u ?? '').trim();
      return /^https?:\/\//i.test(s) ? s : '';
    }

    let selectedRunSuffix = '';
    let selectedLogKey = 'current';
    let logEs = null;
    let runSelectSyncing = false;
    let lastRunsKey = '';
    let lastAutoMatchedRunSuffix = '';
    let selectedArticleIds = [];
    let refreshTimer = null;
    let monitorLocked = false;
    let logoutInFlight = false;
    const MONITOR_TOKEN_STORAGE_KEY = 'lingjing_monitor_admin_token';
    const MONITOR_LOGIN_URL_STORAGE_KEY = 'lingjing_monitor_login_url';
    const nativeFetch = window.fetch.bind(window);

    function readMonitorToken() {
      try {
        return String(sessionStorage.getItem(MONITOR_TOKEN_STORAGE_KEY) || '').trim();
      } catch (_) {
        return '';
      }
    }

    function storeMonitorToken(token) {
      const normalized = String(token || '').trim();
      if (!normalized) return '';
      try {
        sessionStorage.setItem(MONITOR_TOKEN_STORAGE_KEY, normalized);
      } catch (_) {}
      return normalized;
    }

    function clearMonitorToken() {
      try {
        sessionStorage.removeItem(MONITOR_TOKEN_STORAGE_KEY);
      } catch (_) {}
    }

    function readMonitorLoginUrl() {
      try {
        return String(sessionStorage.getItem(MONITOR_LOGIN_URL_STORAGE_KEY) || '').trim();
      } catch (_) {
        return '';
      }
    }

    function storeMonitorLoginUrl(url) {
      const normalized = safeHttpUrl(url);
      if (!normalized) return '';
      try {
        sessionStorage.setItem(MONITOR_LOGIN_URL_STORAGE_KEY, normalized);
      } catch (_) {}
      return normalized;
    }

    function clearMonitorLoginUrl() {
      try {
        sessionStorage.removeItem(MONITOR_LOGIN_URL_STORAGE_KEY);
      } catch (_) {}
    }

    function buildMonitorLoginUrl() {
      const stored = readMonitorLoginUrl();
      if (stored) return stored;
      const referrer = safeHttpUrl(document.referrer || '');
      if (referrer) {
        try {
          return new URL('/login', referrer).toString();
        } catch (_) {}
      }
      try {
        return new URL('/login', window.location.origin).toString();
      } catch (_) {
        return '/login';
      }
    }

    function captureMonitorTokenFromHash() {
      const hash = String(window.location.hash || '').replace(/^#/, '').trim();
      if (!hash) return readMonitorToken();
      const params = new URLSearchParams(hash);
      const token = storeMonitorToken(params.get('token') || '');
      storeMonitorLoginUrl(params.get('login') || '');
      if (token && window.history && typeof window.history.replaceState === 'function') {
        const cleanUrl = `${window.location.pathname}${window.location.search}`;
        window.history.replaceState({}, document.title, cleanUrl);
      } else if (token) {
        window.location.hash = '';
      }
      return token || readMonitorToken();
    }

    function renderLockedState(message) {
      if (monitorLocked) return;
      monitorLocked = true;
      stopLogStream();
      if (refreshTimer) {
        window.clearInterval(refreshTimer);
        refreshTimer = null;
      }
      clearMonitorToken();
      const loginUrl = buildMonitorLoginUrl();
      const wrap = document.querySelector('.wrap');
      if (!wrap) return;
      wrap.innerHTML = `
        <div class="card" style="max-width:720px;margin:48px auto;">
          <h1>管理员登录后可访问</h1>
          <p class="muted" style="font-size:14px;line-height:1.9;">
            ${escHtml(message || '当前运营看板只对管理员账号开放。请从主站登录管理员账号后重新进入。')}
          </p>
          <p class="muted">如果你已经登录，请回到主站重新点击“运营看板”入口，让系统重新携带管理员令牌。</p>
          <div style="margin-top:18px;">
            <a class="btn-copy" href="${escHtml(loginUrl)}" style="text-decoration:none;display:inline-flex;align-items:center;">前往登录页</a>
          </div>
        </div>
      `;
    }

    async function logoutMonitor() {
      if (logoutInFlight) return;
      logoutInFlight = true;
      const btn = document.getElementById('logoutBtn');
      const nextUrl = buildMonitorLoginUrl();
      if (btn) {
        btn.disabled = true;
        btn.textContent = '退出中...';
      }
      try {
        const token = readMonitorToken();
        if (token) {
          await nativeFetch('/api/auth/sessions/current', {
            method: 'DELETE',
            headers: buildAuthorizedHeaders(),
          }).catch(() => null);
        }
      } finally {
        clearMonitorToken();
        clearMonitorLoginUrl();
        window.location.assign(nextUrl);
      }
    }

    function buildAuthorizedHeaders(headersInit) {
      const headers = new Headers(headersInit || {});
      const token = readMonitorToken();
      if (token) headers.set('Authorization', `Bearer ${token}`);
      return headers;
    }

    function isMonitorApiRequest(input) {
      if (typeof input === 'string') return input.startsWith('/api/');
      const url = String(input && input.url ? input.url : '');
      return url.startsWith('/api/') || url.startsWith(`${window.location.origin}/api/`);
    }

    window.fetch = async (input, init = {}) => {
      const shouldAttachAuth = isMonitorApiRequest(input);
      const response = await nativeFetch(input, shouldAttachAuth
        ? { ...init, headers: buildAuthorizedHeaders(init.headers) }
        : init);
      if (shouldAttachAuth && (response.status === 401 || response.status === 403)) {
        renderLockedState('管理员身份校验失败，当前看板不会继续加载数据。');
      }
      return response;
    };

    function createAuthorizedEventSource(path) {
      const token = readMonitorToken();
      if (!token) {
        renderLockedState('缺少管理员令牌，请从主站重新进入运营看板。');
        return null;
      }
      const separator = path.includes('?') ? '&' : '?';
      return new EventSource(`${path}${separator}token=${encodeURIComponent(token)}`);
    }

    function updatePushUi() {
      const hintEl = document.getElementById('pushPickHint');
      const btnEl = document.getElementById('pushSelectedBtn');
      const wechatBtnEl = document.getElementById('wechatPublishBtn');
      const emailBtnEl = document.getElementById('emailSendBtn');
      if (hintEl) hintEl.textContent = `已选 ${selectedArticleIds.length} / 3`;
      if (btnEl) btnEl.disabled = selectedArticleIds.length !== 3 || issueSending;
      if (wechatBtnEl) wechatBtnEl.disabled = selectedArticleIds.length !== 3 || issueWechatSending;
      if (emailBtnEl) emailBtnEl.disabled = selectedArticleIds.length !== 3 || issueEmailSending;
    }

    function setPushStatus(message, isError = false) {
      const el = document.getElementById('pushStatus');
      if (!el) return;
      el.textContent = String(message || '');
      el.style.color = isError ? '#c0392b' : '#5f6f89';
    }

    function setWechatStatus(message, isError = false) {
      const el = document.getElementById('wechatStatus');
      if (!el) return;
      el.textContent = String(message || '');
      el.style.color = isError ? '#c0392b' : '#5f6f89';
    }

    function setEmailStatus(message, isError = false) {
      const el = document.getElementById('emailStatus');
      if (!el) return;
      el.textContent = String(message || '');
      el.style.color = isError ? '#c0392b' : '#5f6f89';
    }

    function toggleArticleSelection(articleId, checked) {
      const id = Number(articleId || 0);
      if (!id) return false;

      if (checked) {
        if (selectedArticleIds.includes(id)) {
          updatePushUi();
          return true;
        }
        if (selectedArticleIds.length >= 3) {
          setPushStatus('Only 3 articles can be selected each time.', true);
          updatePushUi();
          return false;
        }
        selectedArticleIds = [...selectedArticleIds, id];
      } else {
        selectedArticleIds = selectedArticleIds.filter((item) => item !== id);
      }

      setPushStatus('');
      updatePushUi();
      return true;
    }
    let issueSending = false;
    let issueWechatSending = false;
    let issueEmailSending = false;
    const selectedIssueIds = [];
    const selectedIssueMap = new Map();
    const latestArticleLookup = new Map();

    function stopLogStream() {
      if (logEs) {
        logEs.close();
        logEs = null;
      }
    }

    const AI_DIGEST_LABEL = 'AI资讯速览';
    const AI_DIGEST_KEY = 'ai_digest';

    function scoreFmt(x) {
      const n = Number(x || 0);
      return (n > 0 && n < 1) ? n.toFixed(4) : n.toFixed(2);
    }

    /** 仅 AI资讯速览 显示 MAX；其余显示 P/T/排序分（来自刷新时刻推算） */
    function renderBoardArticles(articles, tableId) {
      const g = (articles && articles[0] && articles[0].hn_g != null) ? articles[0].hn_g : '';
      const gLabel = g !== '' ? ` title="当前 G=${g}"` : '';
      const titleRows = (articles || []).map((i) => {
        const articleId = Number(i.id || 0);
        if (articleId > 0) latestArticleLookup.set(articleId, i);
        const href = safeHttpUrl(i.url);
        const titleHtml = href
          ? `<a class="title-link" href="${escHtml(href)}" target="_blank" rel="noopener noreferrer">${escHtml(i.title)}</a>`
          : escHtml(i.title);
        const isDigest = i.source_key === AI_DIGEST_KEY
          || String(i.source || '').trim() === AI_DIGEST_LABEL;
        const picked = articleId > 0 && selectedArticleIds.includes(articleId);
        let pCell, tCell, sCell;
        if (isDigest) {
          pCell = '<td class="num max-cell" title="策展源置顶优先级">MAX</td>';
          tCell = '<td class="num max-cell" title="策展源置顶优先级">MAX</td>';
          sCell = '<td class="num max-cell" title="策展源置顶优先级">MAX</td>';
        } else {
          const p = i.hn_p != null ? Number(i.hn_p).toFixed(2) : '-';
          const t = i.hn_t != null ? Number(i.hn_t).toFixed(2) : '-';
          pCell = `<td class="num" title="点数 P≈热度+1">${p}</td>`;
          tCell = `<td class="num" title="距发布时间 T（小时）">${t}</td>`;
          sCell = `<td class="num">${scoreFmt(i.total_score)}</td>`;
        }
        const pickCell = `<td class="pick-col"><input class="article-pick" type="checkbox" data-article-id="${articleId}" ${picked ? 'checked' : ''} ${articleId > 0 ? '' : 'disabled'}></td>`;
        return `<tr>${pickCell}<td class="source-col">${escHtml(i.source)}</td><td class="title-col">${titleHtml}</td>${pCell}${tCell}${sCell}</tr>`;
      }).join('');
      const el = document.getElementById(tableId);
      el.innerHTML =
        '<tr><th class="pick-col">选</th><th class="source-col">来源</th><th class="title-col">标题</th><th class="num"' + gLabel + '>P</th><th class="num">T</th><th class="num">分数</th></tr>' + titleRows;
    }

    function renderDist(rows, tableId) {
      const distRows = (rows || []).map(i => `<tr><td>${escHtml(i.source)}</td><td>${i.count}</td></tr>`).join('');
      const el = document.getElementById(tableId);
      el.innerHTML = '<tr><th>来源</th><th>数量</th></tr>' + distRows;
    }

    async function parseApiPayload(response) {
      const rawText = await response.text();
      if (!rawText || !rawText.trim()) {
        return {};
      }
      try {
        return JSON.parse(rawText);
      } catch (error) {
        return { detail: rawText };
      }
    }

    async function pushSelectedArticles() {
      if (issueSending || selectedArticleIds.length !== 3) return;

      issueSending = true;
      setPushStatus('正在提交...');
      updatePushUi();

      try {
        const response = await fetch('/api/push/selected', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ articleIds: selectedArticleIds }),
        });
        const data = await parseApiPayload(response);
        if (!response.ok) {
          throw new Error(data?.detail || data?.error || 'push failed');
        }
        const payload = data?.data || data || {};
        const sent = Number(payload?.sent ?? 0);
        const issueId = String(payload?.issue?.id || '');
        const reason = String(payload?.reason || '');
        const suffix = reason ? ` (${reason})` : '';
        setPushStatus(`Push sent to ${sent} devices${issueId ? `, issue=${issueId}` : ''}${suffix}`);
      } catch (error) {
        setPushStatus(error?.message || 'Push failed', true);
      } finally {
        issueSending = false;
        updatePushUi();
      }
    }

    async function publishSelectedArticlesToWechat() {
      if (issueWechatSending || selectedArticleIds.length !== 3) return;

      issueWechatSending = true;
      setWechatStatus('Publishing to WeChat...');
      updatePushUi();

      try {
        const response = await fetch('/api/push/selected/wechat-publish', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ articleIds: selectedArticleIds }),
        });
        const data = await parseApiPayload(response);
        if (!response.ok) {
          throw new Error(data?.detail || data?.error || 'wechat publish failed');
        }
        const payload = data?.data || data || {};
        if (payload?.draftOnly) {
          const issueId = String(payload?.issue?.id || payload?.issueId || '');
          const draftCount = Number(payload?.draftCount ?? (Array.isArray(payload?.draftMediaIds) ? payload.draftMediaIds.length : 0));
          setWechatStatus(
            `WeChat drafts created=${draftCount}${issueId ? `, issue=${issueId}` : ''}`
          );
          return;
        }
        const publishId = String(payload?.publishId || '');
        const issueId = String(payload?.issue?.id || payload?.issueId || '');
        const statusText = String(payload?.status?.statusLabel || payload?.status?.status || 'submitted');
        setWechatStatus(
          `WeChat publish submitted${publishId ? `, publishId=${publishId}` : ''}${issueId ? `, issue=${issueId}` : ''}, status=${statusText}`
        );
      } catch (error) {
        setWechatStatus(error?.message || 'wechat publish failed', true);
      } finally {
        issueWechatSending = false;
        updatePushUi();
      }
    }

    async function sendSelectedArticlesByEmail() {
      if (issueEmailSending || selectedArticleIds.length !== 3) return;

      issueEmailSending = true;
      setEmailStatus('Sending email digest...');
      updatePushUi();

      try {
        const response = await fetch('/api/push/selected/email-send', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ articleIds: selectedArticleIds }),
        });
        const data = await parseApiPayload(response);
        if (!response.ok) {
          throw new Error(data?.detail || data?.error || 'email send failed');
        }
        const payload = data?.data || data || {};
        const sent = Number(payload?.sent ?? 0);
        const failed = Number(payload?.failedCount ?? 0);
        const issueId = String(payload?.issue?.id || '');
        const statusText = String(payload?.status || '');
        setEmailStatus(
          `Email sent=${sent}, failed=${failed}${issueId ? `, issue=${issueId}` : ''}${statusText ? `, status=${statusText}` : ''}`
        );
      } catch (error) {
        setEmailStatus(error?.message || 'email send failed', true);
      } finally {
        issueEmailSending = false;
        updatePushUi();
      }
    }

    function renderRunSummary(run, runLogMatch) {
      const el = document.getElementById('runSummary');
      if (!run) {
        el.textContent = 'No paired run yet.';
        return;
      }
      const biz = Number(run.biz_count ?? 0);
      const personal = Number(run.personal_count ?? 0);
      const total = biz + personal;
      const generated = runLogMatch && runLogMatch.matched
        ? Number(runLogMatch.generated_images ?? 0)
        : null;
      const logLabel = runLogMatch && runLogMatch.matched
        ? `auto log: ${runLogMatch.log_key}`
        : 'auto log: not found';
      el.textContent =
        `news ${total} (biz ${biz} / personal ${personal})`
        + (generated == null ? '; generated images pending log match.' : `; generated images ${generated}.`)
        + ` ${logLabel}`;
    }

    function runSuffixMatch(runList, candidate) {
      const c = String(candidate ?? '');
      return (runList || []).some((r) => String(r.suffix ?? '') === c);
    }

    function syncRunSelect(runs, latestBatchId) {
      const sel = document.getElementById('runSelect');
      const raw = runs || [];
      const sig = JSON.stringify(raw.map((r) => r.suffix ?? ''));
      const preserve = String(selectedRunSuffix || sel.value || '');
      if (sig === lastRunsKey && sel.options.length > 0) {
        if (preserve && runSuffixMatch(raw, preserve)) sel.value = preserve;
        return;
      }
      lastRunsKey = sig;
      runSelectSyncing = true;
      try {
        if (!raw.length) {
          sel.innerHTML = '<option value="">（暂无双榜批次）</option>';
          selectedRunSuffix = '';
          return;
        }
        sel.innerHTML = raw.map((r) => {
          const suf = r.suffix ?? '';
          const t = (r.updated_at || '').replace('T', ' ').slice(0, 19);
          const label = `${suf} · 企业 ${Number(r.biz_count ?? 0)} / 个人 ${Number(r.personal_count ?? 0)} · ${t}`;
          return `<option value="${escHtml(suf)}">${escHtml(label)}</option>`;
        }).join('');
        let pick = preserve && runSuffixMatch(raw, preserve) ? preserve : '';
        if (!pick && latestBatchId) {
          const lid = String(latestBatchId);
          let sufFromLatest = '';
          if (lid.startsWith('biz_')) sufFromLatest = lid.slice(4);
          else if (lid.startsWith('personal_')) sufFromLatest = lid.slice(9);
          if (sufFromLatest && runSuffixMatch(raw, sufFromLatest)) pick = sufFromLatest;
        }
        if (!pick && raw[0]) pick = raw[0].suffix ?? '';
        if (pick) sel.value = pick;
        selectedRunSuffix = sel.value || '';
      } finally {
        runSelectSyncing = false;
      }
    }

    function syncLogSessionSelect(sessions) {
      const sel = document.getElementById('logSessionSelect');
      const cur = selectedLogKey;
      sel.innerHTML = (sessions || []).map((s) =>
        `<option value="${escHtml(s.key)}">${escHtml(s.label)}</option>`
      ).join('');
      const keys = (sessions || []).map((s) => s.key);
      if (cur && keys.includes(cur)) sel.value = cur;
      else { sel.value = 'current'; selectedLogKey = 'current'; }
    }

    async function loadHistoricalLog(key) {
      stopLogStream();
      const logEl = document.getElementById('logs');
      const hint = document.getElementById('logHint');
      hint.textContent = '历史日志（快照，不实时追加）';
      try {
        const r = await fetch('/api/logs/content?key=' + encodeURIComponent(key || 'current'));
        const data = await r.json();
        logEl.textContent = data.text || (r.ok ? '' : '(无法加载)');
        if (data.truncated) hint.textContent += ' · 已截断末尾展示';
      } catch (_) {
        logEl.textContent = '(加载失败)';
      }
    }

    function startLiveLogStream() {
      stopLogStream();
      const logEl = document.getElementById('logs');
      const hint = document.getElementById('logHint');
      hint.textContent = '当前会话（实时追加）';
      let initialized = false;
      logEs = createAuthorizedEventSource('/api/logs/stream?key=current');
      if (!logEs) return;
      logEs.onmessage = (evt) => {
        try {
          const payload = JSON.parse(evt.data || '{}');
          if (payload.type === 'init') {
            logEl.textContent = payload.chunk || '(暂无日志)';
            initialized = true;
          } else if (payload.type === 'append') {
            if (!initialized) {
              logEl.textContent = '';
              initialized = true;
            }
            logEl.textContent += payload.chunk || '';
          }
          logEl.scrollTop = logEl.scrollHeight;
        } catch (_) {}
      };
      logEs.onerror = () => {
        if (!readMonitorToken()) return;
      };
    }

    function applyLogSession(key) {
      selectedLogKey = key || 'current';
      if (selectedLogKey === 'current') startLiveLogStream();
      else loadHistoricalLog(selectedLogKey);
    }

    function updateIssueSelectionInfo() {
      const el = document.getElementById('issueSelectionInfo');
      if (el) el.textContent = `已选 ${selectedIssueIds.length} / 3`;
    }

    function setIssuePushHint(text, isError) {
      const el = document.getElementById('issuePushHint');
      if (!el) return;
      el.textContent = text || '';
      el.style.color = isError ? '#c0392b' : '#5f6f89';
    }

    function clearIssueSelection() {
      selectedIssueIds.splice(0, selectedIssueIds.length);
      selectedIssueMap.clear();
      document.querySelectorAll('.pick-news').forEach((node) => {
        node.checked = false;
      });
      updateIssueSelectionInfo();
      setIssuePushHint('', false);
    }

    function syncIssuePushRows(articles) {
      latestArticleLookup.clear();
      const urlToId = new Map();
      (articles || []).forEach((item) => {
        const id = Number(item.id || 0);
        if (id <= 0) return;
        latestArticleLookup.set(id, item);
        const url = String(item.url || '').trim();
        if (url) urlToId.set(url, id);
      });

      ['titlesBiz', 'titlesPersonal'].forEach((tableId) => {
        const table = document.getElementById(tableId);
        if (!table) return;
        const rows = Array.from(table.querySelectorAll('tr'));
        if (!rows.length) return;

        const header = rows[0];
        if (!header.querySelector('th.pick-col')) {
          const th = document.createElement('th');
          th.className = 'pick-col';
          th.textContent = '选择';
          header.insertBefore(th, header.firstChild);
        }

        rows.slice(1).forEach((row) => {
          const link = row.querySelector('a.title-link');
          const href = link ? String(link.getAttribute('href') || '').trim() : '';
          const articleId = Number(urlToId.get(href) || row.getAttribute('data-news-id') || 0);

          row.setAttribute('data-news-id', articleId > 0 ? String(articleId) : '');
          let firstCell = row.querySelector('td.pick-col');
          if (!firstCell) {
            firstCell = document.createElement('td');
            firstCell.className = 'pick-col';
            row.insertBefore(firstCell, row.firstChild);
          }

          if (articleId > 0) {
            firstCell.innerHTML = `<input type="checkbox" class="pick-news" data-news-id="${articleId}" ${selectedIssueMap.has(articleId) ? 'checked' : ''} />`;
          } else {
            firstCell.textContent = '-';
          }
        });
      });

      updateIssueSelectionInfo();
    }

    function bindIssuePickerEvents() {
      document.addEventListener('change', (evt) => {
        const node = evt.target;
        if (!node || !node.classList || !node.classList.contains('pick-news')) return;
        const articleId = Number(node.getAttribute('data-news-id') || 0);
        if (!articleId) return;

        if (node.checked) {
          if (!selectedIssueMap.has(articleId) && selectedIssueIds.length >= 3) {
            node.checked = false;
            setIssuePushHint('只能勾选 3 条新闻', true);
            return;
          }
          if (!selectedIssueMap.has(articleId)) selectedIssueIds.push(articleId);
          selectedIssueMap.set(articleId, latestArticleLookup.get(articleId) || { id: articleId });
        } else {
          selectedIssueMap.delete(articleId);
          const idx = selectedIssueIds.indexOf(articleId);
          if (idx >= 0) selectedIssueIds.splice(idx, 1);
        }
        updateIssueSelectionInfo();
      });

      const sendBtn = document.getElementById('sendIssueBtn');
      if (sendBtn) {
        sendBtn.addEventListener('click', async () => {
          if (issueSending) return;
          if (selectedIssueIds.length !== 3) {
            setIssuePushHint('请先勾选 3 条新闻', true);
            return;
          }
          issueSending = true;
          sendBtn.disabled = true;
          const label = sendBtn.textContent;
          sendBtn.textContent = '发送中...';
          try {
            const resp = await fetch('/api/push/selected', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ articleIds: selectedIssueIds.slice(0, 3) }),
            });
            const data = await resp.json();
            if (!resp.ok) {
              throw new Error(data.detail || data.error || '发送失败');
            }
            setIssuePushHint(`发送完成，推送 ${Number(data.sent || 0)} 台设备`, false);
            clearIssueSelection();
          } catch (err) {
            setIssuePushHint(String(err && err.message ? err.message : err), true);
          } finally {
            issueSending = false;
            sendBtn.disabled = false;
            sendBtn.textContent = label;
          }
        });
      }

      const clearBtn = document.getElementById('clearIssueBtn');
      if (clearBtn) clearBtn.addEventListener('click', clearIssueSelection);
    }

    async function refresh() {
      const [st, batches, ls] = await Promise.all([
        fetch('/api/status').then((r) => r.json()),
        fetch('/api/batches').then((r) => r.json()),
        fetch('/api/logs/sessions').then((r) => r.json()),
      ]);

      const lb = st.latest_batch || {};
      const latestId = lb.crawl_batch || '';
      document.getElementById('status').innerHTML = `
        <div>当前时间: ${st.now || '-'} <span class="muted">${st.timezone || ''}</span></div>
        <div>数据库: ${st.db_exists ? '已连接' : '未找到'}</div>
        <div>Worker进程: ${st.worker_online ? '在线' : '离线'}</div>
        <div>最新批次: ${latestId || '-'}</div>
        <div>最新批次文章数: ${lb.count ?? 0}</div>
        <div>最近更新时间: ${lb.updated_at || '-'}</div>
      `;

      const runs = batches.runs || [];
      syncRunSelect(runs, latestId);
      selectedRunSuffix = document.getElementById('runSelect').value;
      const run = runs.find((r) => String(r.suffix) === String(selectedRunSuffix)) || runs[0] || null;
      const runChanged = String(run && run.suffix ? run.suffix : '') !== String(lastAutoMatchedRunSuffix || '');

      let runLogMatch = null;
      if (run) {
        const qs = new URLSearchParams({
          suffix: String(run.suffix || ''),
          biz_batch: String(run.biz_batch || ''),
          personal_batch: String(run.personal_batch || ''),
        });
        try {
          runLogMatch = await fetch('/api/runs/log_match?' + qs.toString()).then((r) => r.json());
        } catch (_) {
          runLogMatch = null;
        }
      }
      renderRunSummary(run, runLogMatch);

      let bizBv = { source_distribution: [], articles: [] };
      let perBv = { source_distribution: [], articles: [] };
      if (run && run.biz_batch) {
        bizBv = await fetch('/api/batch_view?batch=' + encodeURIComponent(run.biz_batch)).then((r) => r.json());
      }
      if (run && run.personal_batch) {
        perBv = await fetch('/api/batch_view?batch=' + encodeURIComponent(run.personal_batch)).then((r) => r.json());
      }
      renderDist(bizBv.source_distribution || [], 'distBiz');
      renderDist(perBv.source_distribution || [], 'distPersonal');
      latestArticleLookup.clear();
      renderBoardArticles(bizBv.articles || [], 'titlesBiz');
      renderBoardArticles(perBv.articles || [], 'titlesPersonal');
      selectedArticleIds = selectedArticleIds.filter((id) => latestArticleLookup.has(id));
      updatePushUi();

      const hintB = document.getElementById('batchHint');
      if (run) {
        hintB.textContent = '当前轮次 「' + (run.suffix || '-') + '」；企业批次「' + (run.biz_batch || '—') + '」；个人批次「' + (run.personal_batch || '—') + '」。仅「' + AI_DIGEST_LABEL + '」显示 MAX，其余为公式分；速览条目标题链向 Sources 首条外链。';
      } else {
        hintB.textContent = '暂无爬取批次。完成一次分流双榜任务后将显示企业与个人两栏。';
      }

      const prevKey = selectedLogKey;
      const sessions = ls.sessions || [{ key: 'current', label: '当前会话（第 1 次）' }];
      const shouldAutoJump = !!(run && runChanged && runLogMatch && runLogMatch.matched);
      if (shouldAutoJump) {
        selectedLogKey = runLogMatch.log_key || selectedLogKey;
      }
      syncLogSessionSelect(sessions);
      if (shouldAutoJump) {
        const keys = sessions.map((s) => s.key);
        if (keys.includes(runLogMatch.log_key)) {
          document.getElementById('logSessionSelect').value = runLogMatch.log_key;
          selectedLogKey = runLogMatch.log_key;
          lastAutoMatchedRunSuffix = String(run.suffix || '');
        }
      } else {
        selectedLogKey = document.getElementById('logSessionSelect').value;
      }
      if (selectedLogKey !== prevKey) {
        applyLogSession(selectedLogKey);
      }
    }

    document.getElementById('runSelect').addEventListener('change', () => {
      if (runSelectSyncing) return;
      selectedRunSuffix = document.getElementById('runSelect').value;
      refresh();
    });

    document.getElementById('logSessionSelect').addEventListener('change', () => {
      selectedLogKey = document.getElementById('logSessionSelect').value;
      applyLogSession(selectedLogKey);
    });

    document.addEventListener('change', (event) => {
      const target = event.target;
      if (!target || !target.classList || !target.classList.contains('article-pick')) return;
      const ok = toggleArticleSelection(target.getAttribute('data-article-id'), !!target.checked);
      if (!ok) target.checked = false;
    });

    const pushSelectedBtn = document.getElementById('pushSelectedBtn');
    if (pushSelectedBtn) {
      pushSelectedBtn.addEventListener('click', () => {
        pushSelectedArticles();
      });
    }

    const wechatPublishBtn = document.getElementById('wechatPublishBtn');
    if (wechatPublishBtn) {
      wechatPublishBtn.addEventListener('click', () => {
        publishSelectedArticlesToWechat();
      });
    }

    const emailSendBtn = document.getElementById('emailSendBtn');
    if (emailSendBtn) {
      emailSendBtn.addEventListener('click', () => {
        sendSelectedArticlesByEmail();
      });
    }

    const logoutBtn = document.getElementById('logoutBtn');
    if (logoutBtn) {
      logoutBtn.addEventListener('click', () => {
        logoutMonitor();
      });
    }

    updatePushUi();
    captureMonitorTokenFromHash();

    const logEl0 = document.getElementById('logs');
    if (!readMonitorToken()) {
      renderLockedState('当前运营看板只对管理员开放，请从主站使用管理员账号进入。');
    } else {
      refresh().catch(() => {});
      refreshTimer = window.setInterval(() => {
        if (monitorLocked) return;
        refresh().catch(() => {});
      }, 5000);
      startLiveLogStream();
    }

    const copyBtn = document.getElementById('copyLogs');
    if (copyBtn) copyBtn.addEventListener('click', async () => {
      const text = logEl0.textContent || '';
      const label = copyBtn.textContent;
      try {
        await navigator.clipboard.writeText(text);
        copyBtn.textContent = '已复制';
        setTimeout(() => { copyBtn.textContent = label; }, 1500);
      } catch (_) {
        try {
          const ta = document.createElement('textarea');
          ta.value = text;
          ta.style.position = 'fixed';
          ta.style.left = '-9999px';
          document.body.appendChild(ta);
          ta.select();
          document.execCommand('copy');
          document.body.removeChild(ta);
          copyBtn.textContent = '已复制';
          setTimeout(() => { copyBtn.textContent = label; }, 1500);
        } catch (__) {
          copyBtn.textContent = '失败';
          setTimeout(() => { copyBtn.textContent = label; }, 1500);
        }
      }
    });
  </script>
</body>
</html>
"""
    return _repair_utf8_mojibake_by_line(html)
