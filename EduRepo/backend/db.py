from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_DATA_DIR = Path(__file__).resolve().parent / "data"
_DATA_DIR.mkdir(parents=True, exist_ok=True)
_DB_PATH = _DATA_DIR / "edurepo.db"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _raw_hash(title: str, summary: str, content: str) -> str:
    blob = "\n".join([title or "", summary or "", content or ""]).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()


def init_db() -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS edurepo_articles (
                news_id INTEGER PRIMARY KEY,
                title TEXT,
                summary TEXT,
                content TEXT,
                source TEXT,
                url TEXT,
                cover_url TEXT,
                published_at TEXT,
                in_main INTEGER,
                in_sub INTEGER,
                status TEXT,
                raw_hash TEXT,
                llm_attempts INTEGER,
                llm_error TEXT,
                synced_at TEXT,
                processed_at TEXT,
                translated_title TEXT,
                translated_summary TEXT,
                translated_content TEXT,
                ps_title TEXT,
                ps_summary TEXT,
                ps_markdown TEXT,
                keywords_json TEXT,
                highlights_json TEXT,
                glossary_json TEXT,
                lang TEXT,
                llm_model TEXT,
                raw_json TEXT,
                updated_at TEXT
            )
            """
        )

        cursor = conn.execute("PRAGMA table_info(edurepo_articles)")
        cols = {row[1] for row in cursor.fetchall()}
        add_cols: list[tuple[str, str, Any]] = [
            ("in_main", "INTEGER", 0),
            ("in_sub", "INTEGER", 0),
            ("status", "TEXT", ""),
            ("raw_hash", "TEXT", ""),
            ("llm_attempts", "INTEGER", 0),
            ("llm_error", "TEXT", ""),
            ("synced_at", "TEXT", ""),
            ("processed_at", "TEXT", ""),
            ("ps_title", "TEXT", ""),
            ("ps_summary", "TEXT", ""),
            ("ps_markdown", "TEXT", ""),
            ("keywords_json", "TEXT", ""),
            ("highlights_json", "TEXT", ""),
            ("glossary_json", "TEXT", ""),
            ("lang", "TEXT", ""),
            ("llm_model", "TEXT", ""),
        ]
        for name, typ, default in add_cols:
            if name in cols:
                continue
            if typ == "INTEGER":
                conn.execute(f"ALTER TABLE edurepo_articles ADD COLUMN {name} {typ} DEFAULT {int(default)}")
            else:
                conn.execute(f"ALTER TABLE edurepo_articles ADD COLUMN {name} {typ} DEFAULT ''")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_edurepo_status ON edurepo_articles(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_edurepo_synced_at ON edurepo_articles(synced_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_edurepo_processed_at ON edurepo_articles(processed_at)")
        conn.commit()


def _parse_json_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(row)
    for key in ("raw_json", "keywords_json", "highlights_json", "glossary_json"):
        if not data.get(key):
            data[key] = [] if key.endswith("_json") else {}
            continue
        try:
            data[key] = json.loads(data[key])
        except Exception:
            data[key] = [] if key.endswith("_json") else {}
    return data


def get_article(news_id: int) -> Optional[Dict[str, Any]]:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM edurepo_articles WHERE news_id = ? LIMIT 1", (int(news_id),)).fetchone()
        if not row:
            return None
        return _parse_json_fields(dict(row))


def save_raw_article(article: Dict[str, Any], in_main: bool = False, in_sub: bool = False) -> Tuple[bool, str]:
    """
    Save Crawl raw article into DB. Returns (changed, status).
    When raw content changes, status becomes 'pending' and ps_* fields are cleared.
    """
    now = _now_iso()
    news_id = int(article.get("id") or article.get("news_id") or 0)
    if not news_id:
        raise ValueError("missing news_id")

    title = str(article.get("title") or "")
    summary = str(article.get("summary") or "")
    content = str(article.get("content") or "")
    new_hash = _raw_hash(title, summary, content)

    existing = get_article(news_id) or {}
    old_hash = str(existing.get("raw_hash") or "")
    changed = (not old_hash) or (old_hash != new_hash)
    status = "pending" if changed or not (existing.get("ps_summary") and existing.get("ps_markdown")) else "done"

    raw_json = json.dumps(article, ensure_ascii=False)
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO edurepo_articles (
                news_id, title, summary, content, source, url, cover_url,
                published_at,
                in_main, in_sub, status, raw_hash, llm_attempts, llm_error, synced_at, processed_at,
                translated_title, translated_summary, translated_content,
                ps_title, ps_summary, ps_markdown,
                keywords_json, highlights_json, glossary_json,
                lang, llm_model,
                raw_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(news_id) DO UPDATE SET
                title = excluded.title,
                summary = excluded.summary,
                content = excluded.content,
                source = excluded.source,
                url = excluded.url,
                cover_url = excluded.cover_url,
                published_at = excluded.published_at,
                in_main = CASE WHEN excluded.in_main=1 THEN 1 ELSE in_main END,
                in_sub = CASE WHEN excluded.in_sub=1 THEN 1 ELSE in_sub END,
                status = excluded.status,
                raw_hash = excluded.raw_hash,
                llm_error = CASE WHEN excluded.status='pending' THEN '' ELSE llm_error END,
                synced_at = excluded.synced_at,
                processed_at = CASE WHEN excluded.status='pending' THEN '' ELSE processed_at END,
                ps_title = CASE WHEN excluded.status='pending' THEN '' ELSE ps_title END,
                ps_summary = CASE WHEN excluded.status='pending' THEN '' ELSE ps_summary END,
                ps_markdown = CASE WHEN excluded.status='pending' THEN '' ELSE ps_markdown END,
                keywords_json = CASE WHEN excluded.status='pending' THEN '' ELSE keywords_json END,
                highlights_json = CASE WHEN excluded.status='pending' THEN '' ELSE highlights_json END,
                glossary_json = CASE WHEN excluded.status='pending' THEN '' ELSE glossary_json END,
                raw_json = excluded.raw_json,
                updated_at = excluded.updated_at
            """,
            (
                news_id,
                title,
                summary,
                content,
                str(article.get("source") or ""),
                str(article.get("url") or ""),
                str(article.get("cover_url") or ""),
                str(article.get("published_at") or ""),
                1 if in_main else 0,
                1 if in_sub else 0,
                status,
                new_hash,
                int(existing.get("llm_attempts") or 0),
                "",
                now,
                str(existing.get("processed_at") or ""),
                str(existing.get("translated_title") or ""),
                str(existing.get("translated_summary") or ""),
                str(existing.get("translated_content") or ""),
                str(existing.get("ps_title") or ""),
                str(existing.get("ps_summary") or ""),
                str(existing.get("ps_markdown") or ""),
                json.dumps(existing.get("keywords_json") or existing.get("keywords") or [], ensure_ascii=False),
                json.dumps(existing.get("highlights_json") or existing.get("highlights") or [], ensure_ascii=False),
                json.dumps(existing.get("glossary_json") or existing.get("glossary") or [], ensure_ascii=False),
                str(existing.get("lang") or ""),
                str(existing.get("llm_model") or ""),
                raw_json,
                now,
            ),
        )
        conn.commit()
    return changed, status


def mark_llm_processing(news_id: int) -> int:
    now = _now_iso()
    existing = get_article(int(news_id)) or {}
    attempts = int(existing.get("llm_attempts") or 0) + 1
    with _get_conn() as conn:
        conn.execute(
            "UPDATE edurepo_articles SET status='processing', llm_attempts=?, llm_error='', updated_at=? WHERE news_id=?",
            (attempts, now, int(news_id)),
        )
        conn.commit()
    return attempts


def mark_llm_error(news_id: int, error: str) -> None:
    now = _now_iso()
    with _get_conn() as conn:
        conn.execute(
            "UPDATE edurepo_articles SET status='error', llm_error=?, updated_at=? WHERE news_id=?",
            (str(error or "")[:800], now, int(news_id)),
        )
        conn.commit()


def save_processed_article(news_id: int, ps: Dict[str, Any]) -> None:
    now = _now_iso()
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE edurepo_articles SET
                ps_title=?,
                ps_summary=?,
                ps_markdown=?,
                keywords_json=?,
                highlights_json=?,
                glossary_json=?,
                lang=?,
                llm_model=?,
                status='done',
                llm_error='',
                processed_at=?,
                updated_at=?
            WHERE news_id=?
            """,
            (
                str(ps.get("ps_title") or ""),
                str(ps.get("ps_summary") or ""),
                str(ps.get("ps_markdown") or ""),
                json.dumps(ps.get("keywords") or [], ensure_ascii=False),
                json.dumps(ps.get("highlights") or [], ensure_ascii=False),
                json.dumps(ps.get("glossary") or [], ensure_ascii=False),
                str(ps.get("lang") or ""),
                str(ps.get("llm_model") or ""),
                now,
                now,
                int(news_id),
            ),
        )
        conn.commit()


def reset_article_to_pending(news_id: int) -> None:
    """
    Clear processed fields so the item can be re-processed with updated prompts.
    Keeps raw_json and metadata.
    """
    now = _now_iso()
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE edurepo_articles SET
                status='pending',
                llm_attempts=0,
                ps_title='',
                ps_summary='',
                ps_markdown='',
                keywords_json='[]',
                highlights_json='[]',
                glossary_json='[]',
                llm_error='',
                processed_at='',
                updated_at=?
            WHERE news_id=?
            """,
            (now, int(news_id)),
        )
        conn.commit()


def list_ready_articles(limit: int = 40, board: str = "all", q: str = "") -> List[Dict[str, Any]]:
    where = ["status='done'"]
    params: list[Any] = []
    if board == "main":
        where.append("in_main=1")
    elif board == "sub":
        where.append("in_sub=1")
    else:
        where.append("(in_main=1 OR in_sub=1 OR (in_main=0 AND in_sub=0))")

    kw = str(q or "").strip()
    if kw:
        where.append("(ps_title LIKE ? OR ps_summary LIKE ? OR keywords_json LIKE ?)")
        like = f"%{kw}%"
        params.extend([like, like, like])

    sql = f"SELECT * FROM edurepo_articles WHERE {' AND '.join(where)} ORDER BY processed_at DESC, synced_at DESC LIMIT ?"
    params.append(int(limit))
    out: list[Dict[str, Any]] = []
    with _get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        for row in rows:
            out.append(_parse_json_fields(dict(row)))
    return out


def list_pending_articles(limit: int = 20, board: str = "all") -> List[Dict[str, Any]]:
    where = ["status IN ('pending','error')"]
    params: list[Any] = []
    if board == "main":
        where.append("in_main=1")
    elif board == "sub":
        where.append("in_sub=1")

    sql = f"SELECT * FROM edurepo_articles WHERE {' AND '.join(where)} ORDER BY synced_at DESC LIMIT ?"
    params.append(int(limit))

    out: list[Dict[str, Any]] = []
    with _get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        for row in rows:
            out.append(_parse_json_fields(dict(row)))
    return out


def get_stats() -> Dict[str, Any]:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done,
                SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status='processing' THEN 1 ELSE 0 END) AS processing,
                SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error,
                SUM(CASE WHEN in_main=1 THEN 1 ELSE 0 END) AS in_main,
                SUM(CASE WHEN in_sub=1 THEN 1 ELSE 0 END) AS in_sub,
                MAX(synced_at) AS last_synced_at,
                MAX(processed_at) AS last_processed_at
            FROM edurepo_articles
            """
        ).fetchone()
        if not row:
            return {}
        data = dict(row)
        for k in ("total", "done", "pending", "processing", "error", "in_main", "in_sub"):
            try:
                data[k] = int(data.get(k) or 0)
            except Exception:
                data[k] = 0
        return data
