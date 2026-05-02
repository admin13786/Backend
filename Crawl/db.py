"""
SQLite 数据库模块（替代 PostgreSQL 用于本地测试）
- 连接管理
- 建表
- 新闻文章的读写
- 用户账号读写
"""

import aiosqlite
import os
import hashlib
import json
import re
import secrets
from datetime import datetime

from env_loader import load_crawl_env
from tz_display import batch_ts_suffix
from typing import List, Dict, Optional
from url_utils import normalize_article_url

# 默认数据库文件放在 Crawl/db 目录；可通过环境变量覆盖
load_crawl_env()

_base_dir = os.path.dirname(os.path.abspath(__file__))
_default_db_path = os.path.join(_base_dir, "db", "ai_news.db")


def _resolve_db_path(configured_path: str, default_path: str) -> str:
    normalized = str(configured_path or "").strip()
    if not normalized:
        return default_path
    # Allow local Windows runs to reuse the repo DB even when env keeps the Docker path.
    if os.name == "nt" and normalized.replace("\\", "/").startswith("/app/"):
        return default_path
    return normalized


_db_path = _resolve_db_path(os.getenv("AI_NEWS_DB_PATH", _default_db_path), _default_db_path)
_conn: Optional[aiosqlite.Connection] = None
_INVALID_PUSH_CLIENT_IDS = {"null", "undefined", "nil", "none", "cid_not_support"}
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_ALLOWED_USER_ROLES = {"user", "admin"}


def _get_database_url() -> str:
    """返回 SQLite 数据库路径"""
    return f"sqlite:///{_db_path}"


async def _table_columns(table_name: str) -> set[str]:
    cursor = await _conn.execute(f"PRAGMA table_info({table_name})")
    return {str(row[1]) for row in await cursor.fetchall()}


async def _add_column_if_missing(table_name: str, existing: set[str], column_name: str, definition: str) -> None:
    if column_name in existing:
        return
    await _conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
    existing.add(column_name)


async def _ensure_auth_schema() -> None:
    user_cols = await _table_columns("app_users")
    await _add_column_if_missing("app_users", user_cols, "password_hash", "TEXT")
    await _add_column_if_missing("app_users", user_cols, "display_name", "TEXT DEFAULT ''")
    await _add_column_if_missing("app_users", user_cols, "role", "TEXT DEFAULT 'user'")
    await _add_column_if_missing("app_users", user_cols, "created_at", "TIMESTAMP")
    await _add_column_if_missing("app_users", user_cols, "updated_at", "TIMESTAMP")
    await _conn.execute(
        """
        UPDATE app_users
        SET display_name = COALESCE(NULLIF(TRIM(display_name), ''), username)
        WHERE COALESCE(NULLIF(TRIM(display_name), ''), '') = ''
        """
    )
    await _conn.execute(
        """
        UPDATE app_users
        SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
            updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
        """
    )

    session_cols = await _table_columns("app_sessions")
    await _add_column_if_missing("app_sessions", session_cols, "username", "TEXT")
    await _add_column_if_missing("app_sessions", session_cols, "created_at", "TIMESTAMP")
    await _add_column_if_missing("app_sessions", session_cols, "updated_at", "TIMESTAMP")
    await _conn.execute(
        """
        UPDATE app_sessions
        SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
            updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
        """
    )

    conversation_cols = await _table_columns("workshop_conversations")
    await _add_column_if_missing("workshop_conversations", conversation_cols, "title", "TEXT DEFAULT ''")
    await _add_column_if_missing("workshop_conversations", conversation_cols, "data_json", "TEXT DEFAULT '{}'")
    await _add_column_if_missing("workshop_conversations", conversation_cols, "created_at", "TIMESTAMP")
    await _add_column_if_missing("workshop_conversations", conversation_cols, "updated_at", "TIMESTAMP")
    await _conn.execute(
        """
        UPDATE workshop_conversations
        SET title = COALESCE(NULLIF(TRIM(title), ''), id),
            data_json = COALESCE(NULLIF(TRIM(data_json), ''), '{}'),
            created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
            updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
        """
    )


async def init_db():
    """初始化数据库连接并建表"""
    global _conn
    os.makedirs(os.path.dirname(_db_path), exist_ok=True)
    print(f"[DB] connecting: {_db_path}")

    _conn = await aiosqlite.connect(_db_path)

    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS news_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            cover_url TEXT DEFAULT '',
            summary TEXT DEFAULT '',
            source TEXT DEFAULT '',
            source_key TEXT DEFAULT '',
            content TEXT DEFAULT '',
            total_score REAL DEFAULT 0,
            ai_relevance REAL DEFAULT 0,
            industry_impact REAL DEFAULT 0,
            spread_heat REAL DEFAULT 0,
            timeliness REAL DEFAULT 0,
            content_quality REAL DEFAULT 0,
            readability REAL DEFAULT 0,
            rank INTEGER DEFAULT 0,
            crawl_batch TEXT DEFAULT '',
            published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS app_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT DEFAULT '',
            role TEXT DEFAULT 'user',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS app_sessions (
            token TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS workshop_conversations (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            title TEXT NOT NULL,
            data_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS push_devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT DEFAULT '',
            client_id TEXT NOT NULL UNIQUE,
            platform TEXT DEFAULT 'android',
            device_name TEXT DEFAULT '',
            push_enabled INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS push_brief_issues (
            id TEXT PRIMARY KEY,
            issue_date TEXT NOT NULL,
            title TEXT DEFAULT '',
            subtitle TEXT DEFAULT '',
            footer TEXT DEFAULT '',
            selection_count INTEGER DEFAULT 0,
            created_by TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS push_brief_issue_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_id TEXT NOT NULL,
            sort_index INTEGER NOT NULL,
            article_id INTEGER NOT NULL,
            source TEXT DEFAULT '',
            headline TEXT DEFAULT '',
            article_url TEXT DEFAULT '',
            cover_image TEXT DEFAULT '',
            warning TEXT DEFAULT '',
            expanded_body TEXT DEFAULT '',
            brief_json TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(issue_id, sort_index)
        )
    """)
    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS wechat_publish_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_id TEXT NOT NULL,
            draft_media_id TEXT DEFAULT '',
            publish_id TEXT DEFAULT '',
            publish_status INTEGER DEFAULT -1,
            status TEXT DEFAULT '',
            article_id TEXT DEFAULT '',
            article_urls_json TEXT DEFAULT '[]',
            fail_idx_json TEXT DEFAULT '[]',
            error_message TEXT DEFAULT '',
            created_by TEXT DEFAULT '',
            raw_json TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS email_subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            user_id TEXT DEFAULT '',
            display_name TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            source TEXT DEFAULT '',
            unsubscribe_token TEXT NOT NULL UNIQUE,
            verified_at TIMESTAMP,
            unsubscribed_at TIMESTAMP,
            last_sent_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await _conn.execute("""
        CREATE TABLE IF NOT EXISTS email_publish_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_id TEXT NOT NULL,
            subject TEXT DEFAULT '',
            status TEXT DEFAULT '',
            sent_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            created_by TEXT DEFAULT '',
            raw_json TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur = await _conn.execute("PRAGMA table_info(news_articles)")
    cols = {row[1] for row in await cur.fetchall()}
    if "source_key" not in cols:
        await _conn.execute(
            "ALTER TABLE news_articles ADD COLUMN source_key TEXT DEFAULT ''"
        )
    if "cover_url" not in cols:
        await _conn.execute(
            "ALTER TABLE news_articles ADD COLUMN cover_url TEXT DEFAULT ''"
        )
    if "brief_json" not in cols:
        await _conn.execute(
            "ALTER TABLE news_articles ADD COLUMN brief_json TEXT DEFAULT ''"
        )
    # 加索引加速排序查询
    await _ensure_auth_schema()
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_articles_total_score
        ON news_articles (total_score DESC)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_articles_crawl_batch
        ON news_articles (crawl_batch)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_app_users_username
        ON app_users (username)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_app_sessions_username
        ON app_sessions (username)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_workshop_conversations_username
        ON workshop_conversations (username)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_push_devices_client_id
        ON push_devices (client_id)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_push_devices_enabled
        ON push_devices (push_enabled)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_push_brief_issues_date
        ON push_brief_issues (issue_date DESC, updated_at DESC)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_push_brief_issue_items_issue
        ON push_brief_issue_items (issue_id, sort_index)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_wechat_publish_records_issue
        ON wechat_publish_records (issue_id, created_at DESC)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_wechat_publish_records_publish_id
        ON wechat_publish_records (publish_id)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_email_subscribers_status
        ON email_subscribers (status, updated_at DESC)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_email_subscribers_user_id
        ON email_subscribers (user_id)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_email_subscribers_unsubscribe_token
        ON email_subscribers (unsubscribe_token)
    """)
    await _conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_email_publish_records_issue
        ON email_publish_records (issue_id, created_at DESC)
    """)
    await ensure_default_users()
    await _conn.commit()

    print("[DB] ready")


async def close_pool():
    """关闭数据库连接"""
    global _conn
    if _conn:
        await _conn.close()
        _conn = None
        print("[DB] closed")


def _parse_dt(val) -> datetime:
    """将字符串或其他类型转为 datetime 对象"""
    if isinstance(val, datetime):
        return val
    if isinstance(val, str) and val:
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            pass
    return datetime.now()


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    key = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, 200_000)
    return f"{salt.hex()}:{key.hex()}"


def verify_password(password: str, stored: str) -> bool:
    if ":" not in stored:
        return hashlib.sha256(str(password or "").encode("utf-8")).hexdigest() == stored
    salt_hex, key_hex = stored.split(":", 1)
    salt = bytes.fromhex(salt_hex)
    key = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, 200_000)
    return key.hex() == key_hex


def normalize_user_role(role: str | None, username: str | None = None) -> str:
    normalized = str(role or "").strip().lower()
    if normalized in _ALLOWED_USER_ROLES:
        return normalized
    if str(username or "").strip().lower() == "admin":
        return "admin"
    return "user"


async def ensure_default_users():
    defaults = [
        ("admin", "admin123", "管理员"),
        ("workshop_guest", "123456", "默认用户"),
    ]
    for username, password, display_name in defaults:
        role = normalize_user_role(None, username)
        await _conn.execute(
            """
            INSERT INTO app_users (username, password_hash, display_name, role, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(username) DO UPDATE SET
                password_hash = CASE
                    WHEN COALESCE(app_users.password_hash, '') = '' THEN excluded.password_hash
                    ELSE app_users.password_hash
                END,
                display_name = excluded.display_name,
                role = excluded.role,
                updated_at = CURRENT_TIMESTAMP
            """,
            (username, hash_password(password), display_name, role),
        )


async def get_user_by_username(username: str) -> Optional[Dict]:
    if not _conn or not username:
        return None
    cursor = await _conn.execute(
        """
        SELECT id, username, password_hash, display_name, role, created_at, updated_at
        FROM app_users
        WHERE username = ?
        LIMIT 1
        """,
        (username,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    columns = [description[0] for description in cursor.description]
    user = dict(zip(columns, row))
    user["role"] = normalize_user_role(user.get("role"), user.get("username"))
    return user


async def create_user(
    username: str,
    password: str,
    display_name: str = "",
    role: str = "user",
) -> Dict:
    if not _conn:
        raise RuntimeError("database not initialized")
    normalized_username = str(username or "").strip()
    normalized_display_name = str(display_name or normalized_username).strip()
    normalized_role = normalize_user_role(role, normalized_username)
    await _conn.execute(
        """
        INSERT INTO app_users (username, password_hash, display_name, role, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            normalized_username,
            hash_password(password),
            normalized_display_name,
            normalized_role,
        ),
    )
    await _conn.commit()
    user = await get_user_by_username(normalized_username)
    if not user:
        raise RuntimeError("failed to create user")
    return user


async def create_session(username: str, token: str) -> None:
    if not _conn:
        raise RuntimeError("database not initialized")
    await _conn.execute(
        """
        INSERT INTO app_sessions (token, username, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(token) DO UPDATE SET
            username = excluded.username,
            updated_at = CURRENT_TIMESTAMP
        """,
        (token, username),
    )
    await _conn.commit()


async def delete_session(token: str) -> None:
    if not _conn or not token:
        return
    await _conn.execute("DELETE FROM app_sessions WHERE token = ?", (token,))
    await _conn.commit()


async def get_session_user(token: str) -> Optional[Dict]:
    if not _conn or not token:
        return None
    cursor = await _conn.execute(
        """
        SELECT u.id, u.username, u.display_name, u.role, s.token, s.updated_at
        FROM app_sessions s
        JOIN app_users u ON u.username = s.username
        WHERE s.token = ?
        LIMIT 1
        """,
        (token,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    columns = [description[0] for description in cursor.description]
    user = dict(zip(columns, row))
    user["role"] = normalize_user_role(user.get("role"), user.get("username"))
    return user


async def list_workshop_conversations(username: str) -> List[Dict]:
    if not _conn or not username:
        return []
    cursor = await _conn.execute(
        """
        SELECT id, username, title, data_json, created_at, updated_at
        FROM workshop_conversations
        WHERE username = ?
        ORDER BY updated_at DESC, created_at DESC
        """,
        (username,),
    )
    rows = await cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    result = []
    for row in rows:
        item = dict(zip(columns, row))
        payload = json.loads(item.get("data_json") or "{}")
        payload.setdefault("id", item["id"])
        payload.setdefault("title", item["title"])
        payload.setdefault("createdAt", item.get("created_at"))
        payload.setdefault("updatedAt", item.get("updated_at"))
        result.append(payload)
    return result


async def upsert_workshop_conversation(username: str, conversation: Dict) -> Dict:
    if not _conn:
        raise RuntimeError("database not initialized")
    conv_id = str(conversation.get("id") or "").strip()
    if not conv_id:
        raise ValueError("missing conversation id")
    title = str(conversation.get("title") or "新对话").strip() or "新对话"
    conversation_mode = (
        "skill_assistant"
        if str(conversation.get("conversationMode") or "").strip() == "skill_assistant"
        else "workshop"
    )
    payload = {
        "id": conv_id,
        "title": title,
        "conversationMode": conversation_mode,
        "orderIndex": conversation.get("orderIndex"),
        "messages": conversation.get("messages", []),
        "selectedSkills": conversation.get("selectedSkills", []),
        "preview": conversation.get("preview", {}),
        "createdAt": conversation.get("createdAt") or datetime.now().isoformat(),
        "updatedAt": conversation.get("updatedAt") or datetime.now().isoformat(),
    }
    await _conn.execute(
        """
        INSERT INTO workshop_conversations (id, username, title, data_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            username = excluded.username,
            title = excluded.title,
            data_json = excluded.data_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (conv_id, username, title, json.dumps(payload, ensure_ascii=False)),
    )
    await _conn.commit()
    payload["updatedAt"] = datetime.now().isoformat()
    return payload


async def delete_workshop_conversation(username: str, conversation_id: str) -> None:
    if not _conn or not username or not conversation_id:
        return
    await _conn.execute(
        "DELETE FROM workshop_conversations WHERE username = ? AND id = ?",
        (username, conversation_id),
    )
    await _conn.commit()


async def upsert_articles(articles: List[Dict], batch_id: str = "") -> int:
    """
    批量写入新闻文章（URL冲突时更新评分）
    返回写入/更新的条数
    """
    if not _conn or not articles:
        return 0

    if not batch_id:
        batch_id = batch_ts_suffix()

    count = 0
    for i, article in enumerate(articles, 1):
        try:
            normalized_url = normalize_article_url(article.get("url", ""))
            if not normalized_url:
                continue
            params = (
                article.get("title", ""),
                normalized_url,
                article.get("cover_url", ""),
                article.get("summary", ""),
                article.get("source", ""),
                str(article.get("source_key", "") or ""),
                article.get("content", "")[:2000],
                article.get("brief_json", ""),
                float(article.get("total_score", 0)),
                float(article.get("ai_relevance", 0)),
                float(article.get("industry_impact", 0)),
                float(article.get("spread_heat", 0)),
                float(article.get("timeliness", 0)),
                float(article.get("content_quality", 0)),
                float(article.get("readability", 0)),
                i,  # rank
                batch_id,
                _parse_dt(article.get("published_at")),
            )
            await _conn.execute("""
                INSERT INTO news_articles
                    (title, url, cover_url, summary, source, source_key, content,
                     brief_json,
                     total_score, ai_relevance, industry_impact,
                     spread_heat, timeliness, content_quality, readability,
                     rank, crawl_batch, published_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                ON CONFLICT (url) DO UPDATE SET
                    title = EXCLUDED.title,
                    cover_url = CASE
                        WHEN TRIM(COALESCE(EXCLUDED.cover_url, '')) <> '' THEN EXCLUDED.cover_url
                        ELSE news_articles.cover_url
                    END,
                    summary = EXCLUDED.summary,
                    source = EXCLUDED.source,
                    source_key = EXCLUDED.source_key,
                    brief_json = EXCLUDED.brief_json,
                    total_score = EXCLUDED.total_score,
                    ai_relevance = EXCLUDED.ai_relevance,
                    industry_impact = EXCLUDED.industry_impact,
                    spread_heat = EXCLUDED.spread_heat,
                    timeliness = EXCLUDED.timeliness,
                    content_quality = EXCLUDED.content_quality,
                    readability = EXCLUDED.readability,
                    rank = EXCLUDED.rank,
                    crawl_batch = EXCLUDED.crawl_batch,
                    updated_at = CURRENT_TIMESTAMP
            """, params)
            count += 1
        except Exception as e:
            print(f"  [WARN] save failed [{i}]: {e}")

    await _conn.commit()
    print(f"[DB] saved {count} articles (batch: {batch_id})")
    return count


async def get_top_articles(limit: int = 60, batch: str = "", board: str = "") -> List[Dict]:
    """
    从数据库读取排名靠前的文章
    board: "business" 按 biz_ 前缀筛选, "personal" 按 personal_ 前缀筛选
    batch: 指定批次（优先级高于 board）
    """
    if not _conn:
        return []

    cursor = await _conn.cursor()

    if batch:
        await cursor.execute(
            """SELECT * FROM news_articles
               WHERE crawl_batch = ?
               ORDER BY total_score DESC LIMIT ?""",
            (batch, limit)
        )
    elif board == "business":
        await cursor.execute(
            """SELECT * FROM news_articles
               WHERE crawl_batch = (
                 SELECT crawl_batch FROM news_articles
                 WHERE crawl_batch LIKE 'biz_%'
                 ORDER BY updated_at DESC LIMIT 1
               )
               ORDER BY total_score DESC LIMIT ?""",
            (limit,)
        )
    elif board == "personal":
        await cursor.execute(
            """SELECT * FROM news_articles
               WHERE crawl_batch = (
                 SELECT crawl_batch FROM news_articles
                 WHERE crawl_batch LIKE 'personal_%'
                 ORDER BY updated_at DESC LIMIT 1
               )
               ORDER BY total_score DESC LIMIT ?""",
            (limit,)
        )
    else:
        # 取最新批次（兼容旧逻辑）
        await cursor.execute(
            "SELECT crawl_batch FROM news_articles ORDER BY created_at DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        if not row:
            return []
        # 使用列名获取值
        columns = [description[0] for description in cursor.description]
        row_dict = dict(zip(columns, row))
        latest_batch = row_dict.get("crawl_batch", "")
        if not latest_batch:
            return []
        await cursor.execute(
            """SELECT * FROM news_articles
               WHERE crawl_batch = ?
               ORDER BY total_score DESC LIMIT ?""",
            (latest_batch, limit)
        )

    rows = await cursor.fetchall()

    # Convert to dict
    columns = [description[0] for description in cursor.description]
    result = []
    for row in rows:
        result.append(dict(zip(columns, row)))

    return result


async def get_all_batches() -> List[Dict]:
    """获取所有爬取批次"""
    if not _conn:
        return []

    cursor = await _conn.cursor()
    await cursor.execute("""
        SELECT crawl_batch, COUNT(*) as article_count,
               MIN(created_at) as crawl_time
        FROM news_articles
        GROUP BY crawl_batch
        ORDER BY crawl_time DESC
        LIMIT 20
    """)

    rows = await cursor.fetchall()
    return [{"batch": r[0], "count": r[1], "time": r[2]} for r in rows]


async def get_article_by_id(article_id: int) -> Optional[Dict]:
    """按主键 id 获取单条文章。"""
    if not _conn:
        return None
    try:
        aid = int(article_id)
    except Exception:
        return None

    cursor = await _conn.cursor()
    await cursor.execute("SELECT * FROM news_articles WHERE id = ? LIMIT 1", (aid,))
    row = await cursor.fetchone()
    if not row:
        return None
    columns = [description[0] for description in cursor.description]
    return dict(zip(columns, row))


async def get_articles_by_ids(article_ids: List[int]) -> List[Dict]:
    """Return articles in the same order as the requested ids."""
    if not _conn or not article_ids:
        return []

    normalized_ids = []
    for article_id in article_ids:
        try:
            normalized_ids.append(int(article_id))
        except (TypeError, ValueError):
            continue

    if not normalized_ids:
        return []

    placeholders = ",".join("?" for _ in normalized_ids)
    cursor = await _conn.execute(
        f"SELECT * FROM news_articles WHERE id IN ({placeholders})",
        tuple(normalized_ids),
    )
    rows = await cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    row_map = {
        int(item["id"]): item
        for item in (dict(zip(columns, row)) for row in rows)
        if item.get("id") is not None
    }

    ordered = []
    for article_id in normalized_ids:
        article = row_map.get(article_id)
        if article:
            ordered.append(article)
    return ordered


async def upsert_push_device(
    client_id: str,
    user_id: str = "",
    platform: str = "android",
    device_name: str = "",
    push_enabled: bool = True,
) -> Dict:
    if not _conn:
        raise RuntimeError("database not initialized")

    normalized_client_id = str(client_id or "").strip()
    if not normalized_client_id or normalized_client_id.lower() in _INVALID_PUSH_CLIENT_IDS:
        raise ValueError("missing client_id")

    normalized_user_id = str(user_id or "").strip()
    normalized_platform = str(platform or "android").strip() or "android"
    normalized_device_name = str(device_name or "").strip()

    await _conn.execute(
        """
        INSERT INTO push_devices (
            user_id, client_id, platform, device_name, push_enabled, updated_at
        )
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(client_id) DO UPDATE SET
            user_id = excluded.user_id,
            platform = excluded.platform,
            device_name = excluded.device_name,
            push_enabled = excluded.push_enabled,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            normalized_user_id,
            normalized_client_id,
            normalized_platform,
            normalized_device_name,
            1 if push_enabled else 0,
        ),
    )
    await _conn.commit()

    cursor = await _conn.execute(
        """
        SELECT id, user_id, client_id, platform, device_name, push_enabled, created_at, updated_at
        FROM push_devices
        WHERE client_id = ?
        LIMIT 1
        """,
        (normalized_client_id,),
    )
    row = await cursor.fetchone()
    if not row:
        raise RuntimeError("failed to upsert push device")
    columns = [description[0] for description in cursor.description]
    item = dict(zip(columns, row))
    item["push_enabled"] = bool(item.get("push_enabled"))
    return item


async def list_push_devices(platform: str = "", push_enabled_only: bool = False) -> List[Dict]:
    if not _conn:
        return []

    where = []
    params = []

    normalized_platform = str(platform or "").strip()
    if normalized_platform:
        where.append("platform = ?")
        params.append(normalized_platform)
    if push_enabled_only:
        where.append("push_enabled = 1")

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    cursor = await _conn.execute(
        f"""
        SELECT id, user_id, client_id, platform, device_name, push_enabled, created_at, updated_at
        FROM push_devices
        {where_sql}
        ORDER BY updated_at DESC, id DESC
        """,
        tuple(params),
    )
    rows = await cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    result = []
    for row in rows:
        item = dict(zip(columns, row))
        item["push_enabled"] = bool(item.get("push_enabled"))
        result.append(item)
    return result


async def save_push_brief_issue(issue: Dict, created_by: str = "") -> Dict:
    if not _conn:
        raise RuntimeError("database not initialized")

    issue_id = str(issue.get("id") or "").strip()
    if not issue_id:
        raise ValueError("missing issue id")

    issue_date = str(issue.get("date") or issue_id).strip() or issue_id
    title = str(issue.get("title") or "").strip()
    subtitle = str(issue.get("subtitle") or "").strip()
    footer = str(issue.get("footer") or "").strip()
    selection_count = int(issue.get("selectionCount") or 0)
    items = issue.get("items") if isinstance(issue.get("items"), list) else []
    normalized_created_by = str(created_by or "").strip()

    await _conn.execute(
        """
        INSERT INTO push_brief_issues (
            id, issue_date, title, subtitle, footer, selection_count, created_by, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            issue_date = excluded.issue_date,
            title = excluded.title,
            subtitle = excluded.subtitle,
            footer = excluded.footer,
            selection_count = excluded.selection_count,
            created_by = excluded.created_by,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            issue_id,
            issue_date,
            title,
            subtitle,
            footer,
            selection_count,
            normalized_created_by,
        ),
    )
    await _conn.execute(
        "DELETE FROM push_brief_issue_items WHERE issue_id = ?",
        (issue_id,),
    )

    for index, item in enumerate(items, 1):
        article_id = int(item.get("newsId") or item.get("articleId") or 0)
        expanded_body = item.get("expandedBody")
        if isinstance(expanded_body, list):
            expanded_body_text = json.dumps(expanded_body, ensure_ascii=False)
        else:
            expanded_body_text = json.dumps(
                [str(expanded_body).strip()],
                ensure_ascii=False,
            ) if str(expanded_body or "").strip() else "[]"

        brief_json = item.get("brief")
        if not isinstance(brief_json, dict):
            brief_json = {}

        await _conn.execute(
            """
            INSERT INTO push_brief_issue_items (
                issue_id, sort_index, article_id, source, headline, article_url,
                cover_image, warning, expanded_body, brief_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                issue_id,
                index,
                article_id,
                str(item.get("source") or "").strip(),
                str(item.get("headline") or "").strip(),
                str(item.get("articleUrl") or "").strip(),
                str(item.get("coverImage") or "").strip(),
                str(item.get("warning") or "").strip(),
                expanded_body_text,
                json.dumps(brief_json, ensure_ascii=False),
            ),
        )

    await _conn.commit()
    stored_issue = await get_push_brief_issue_by_id(issue_id)
    if not stored_issue:
        raise RuntimeError("failed to save push brief issue")
    return stored_issue


def _deserialize_issue_body(raw_body: str) -> List[str]:
    text = str(raw_body or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        parsed = [text]
    if not isinstance(parsed, list):
        parsed = [str(parsed).strip()]
    return [str(item or "").strip() for item in parsed if str(item or "").strip()]


def _serialize_issue_body(value) -> str:
    if isinstance(value, list):
        items = [str(item or "").strip() for item in value if str(item or "").strip()]
        return json.dumps(items, ensure_ascii=False)
    text = str(value or "").strip()
    if not text:
        return "[]"
    return json.dumps([text], ensure_ascii=False)


async def get_push_brief_issue_by_id(issue_id: str) -> Optional[Dict]:
    if not _conn or not issue_id:
        return None

    cursor = await _conn.execute(
        """
        SELECT id, issue_date, title, subtitle, footer, selection_count, created_by, created_at, updated_at
        FROM push_brief_issues
        WHERE id = ?
        LIMIT 1
        """,
        (str(issue_id).strip(),),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    columns = [description[0] for description in cursor.description]
    issue_row = dict(zip(columns, row))

    items_cursor = await _conn.execute(
        """
        SELECT id, sort_index, article_id, source, headline, article_url, cover_image,
               warning, expanded_body, brief_json
        FROM push_brief_issue_items
        WHERE issue_id = ?
        ORDER BY sort_index ASC, id ASC
        """,
        (str(issue_id).strip(),),
    )
    item_rows = await items_cursor.fetchall()
    item_columns = [description[0] for description in items_cursor.description]
    items = []
    for item_row in item_rows:
        item = dict(zip(item_columns, item_row))
        try:
            brief = json.loads(item.get("brief_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            brief = {}
        items.append(
            {
                "id": f"{issue_row['id']}-{int(item.get('sort_index') or 0)}",
                "newsId": int(item.get("article_id") or 0),
                "source": str(item.get("source") or "").strip(),
                "headline": str(item.get("headline") or "").strip(),
                "warning": str(item.get("warning") or "").strip(),
                "articleUrl": str(item.get("article_url") or "").strip(),
                "coverImage": str(item.get("cover_image") or "").strip(),
                "expandedBody": _deserialize_issue_body(item.get("expanded_body") or ""),
                "brief": brief if isinstance(brief, dict) else {},
            }
        )

    return {
        "id": str(issue_row.get("id") or "").strip(),
        "date": str(issue_row.get("issue_date") or "").strip(),
        "selectionCount": int(issue_row.get("selection_count") or 0),
        "title": str(issue_row.get("title") or "").strip(),
        "subtitle": str(issue_row.get("subtitle") or "").strip(),
        "footer": str(issue_row.get("footer") or "").strip(),
        "createdBy": str(issue_row.get("created_by") or "").strip(),
        "createdAt": issue_row.get("created_at"),
        "updatedAt": issue_row.get("updated_at"),
        "items": items,
    }


async def get_latest_push_brief_issue() -> Optional[Dict]:
    if not _conn:
        return None

    cursor = await _conn.execute(
        """
        SELECT id
        FROM push_brief_issues
        ORDER BY issue_date DESC, updated_at DESC, created_at DESC
        LIMIT 1
        """
    )
    row = await cursor.fetchone()
    if not row:
        return None
    return await get_push_brief_issue_by_id(row[0])


async def get_articles_by_ids(article_ids: List[int]) -> List[Dict]:
    if not _conn:
        return []

    normalized_ids: List[int] = []
    seen = set()
    for value in article_ids or []:
        try:
            article_id = int(value)
        except (TypeError, ValueError):
            continue
        if article_id <= 0 or article_id in seen:
            continue
        seen.add(article_id)
        normalized_ids.append(article_id)

    if not normalized_ids:
        return []

    placeholders = ",".join(["?"] * len(normalized_ids))
    cursor = await _conn.execute(
        f"SELECT * FROM news_articles WHERE id IN ({placeholders})",
        tuple(normalized_ids),
    )
    rows = await cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    by_id = {}
    for row in rows:
        item = dict(zip(columns, row))
        by_id[int(item.get("id"))] = item
    return [by_id[article_id] for article_id in normalized_ids if article_id in by_id]


def _parse_issue_brief(brief_json: str) -> Dict:
    text = str(brief_json or "").strip()
    if not text:
        return {}
    try:
        value = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _build_issue_payload(issue_row: Dict, item_rows: List[Dict]) -> Dict:
    items = []
    for row in item_rows:
        item = {
            "id": row.get("id"),
            "newsId": row.get("article_id"),
            "source": row.get("source", ""),
            "headline": row.get("headline", ""),
            "warning": row.get("warning", ""),
            "articleUrl": row.get("article_url", ""),
            "coverImage": row.get("cover_image", ""),
            "expandedBody": _deserialize_issue_body(row.get("expanded_body", "")),
            "brief": _parse_issue_brief(row.get("brief_json", "")),
        }
        items.append(item)

    return {
        "id": issue_row.get("id"),
        "date": issue_row.get("issue_date"),
        "title": issue_row.get("title", ""),
        "subtitle": issue_row.get("subtitle", ""),
        "footer": issue_row.get("footer", ""),
        "selectionCount": int(issue_row.get("selection_count") or len(items)),
        "createdBy": issue_row.get("created_by", ""),
        "createdAt": issue_row.get("created_at"),
        "updatedAt": issue_row.get("updated_at"),
        "items": items,
    }


async def save_push_brief_issue(
    issue_id: str | Dict,
    issue_date: str = "",
    title: str = "",
    subtitle: str = "",
    footer: str = "",
    created_by: str = "",
    items: Optional[List[Dict]] = None,
) -> Dict:
    if not _conn:
        raise RuntimeError("database not initialized")

    if isinstance(issue_id, dict):
        issue = issue_id
        return await save_push_brief_issue(
            issue_id=str(issue.get("id") or "").strip(),
            issue_date=str(issue.get("date") or issue.get("id") or "").strip(),
            title=str(issue.get("title") or "").strip(),
            subtitle=str(issue.get("subtitle") or "").strip(),
            footer=str(issue.get("footer") or "").strip(),
            created_by=str(created_by or issue.get("createdBy") or "").strip(),
            items=issue.get("items") if isinstance(issue.get("items"), list) else [],
        )

    normalized_issue_id = str(issue_id or "").strip()
    if not normalized_issue_id:
        raise ValueError("missing issue_id")
    normalized_issue_date = str(issue_date or "").strip()
    if not normalized_issue_date:
        raise ValueError("missing issue_date")

    normalized_items = []
    for raw in items or []:
        try:
            article_id = int(raw.get("newsId") or raw.get("article_id") or 0)
        except (TypeError, ValueError):
            continue
        if article_id <= 0:
            continue
        brief_value = raw.get("brief") or {}
        if isinstance(brief_value, dict):
            brief_json = json.dumps(brief_value, ensure_ascii=False)
        else:
            brief_json = str(brief_value or "").strip()
        normalized_items.append(
            {
                "article_id": article_id,
                "source": str(raw.get("source", "") or "").strip(),
                "headline": str(raw.get("headline", "") or "").strip(),
                "warning": str(raw.get("warning", "") or "").strip(),
                "article_url": str(raw.get("articleUrl", "") or "").strip(),
                "cover_image": str(raw.get("coverImage", "") or "").strip(),
                "expanded_body": _serialize_issue_body(raw.get("expandedBody")),
                "brief_json": brief_json,
            }
        )

    if not normalized_items:
        raise ValueError("issue items are empty")

    await _conn.execute(
        """
        INSERT INTO push_brief_issues (
            id, issue_date, title, subtitle, footer, selection_count, created_by, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            issue_date = excluded.issue_date,
            title = excluded.title,
            subtitle = excluded.subtitle,
            footer = excluded.footer,
            selection_count = excluded.selection_count,
            created_by = excluded.created_by,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            normalized_issue_id,
            normalized_issue_date,
            str(title or "").strip(),
            str(subtitle or "").strip(),
            str(footer or "").strip(),
            len(normalized_items),
            str(created_by or "").strip(),
        ),
    )
    await _conn.execute(
        "DELETE FROM push_brief_issue_items WHERE issue_id = ?",
        (normalized_issue_id,),
    )
    for index, item in enumerate(normalized_items, 1):
        await _conn.execute(
            """
            INSERT INTO push_brief_issue_items (
                issue_id, sort_index, article_id, source, headline, article_url,
                cover_image, warning, expanded_body, brief_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                normalized_issue_id,
                index,
                item["article_id"],
                item["source"],
                item["headline"],
                item["article_url"],
                item["cover_image"],
                item["warning"],
                item["expanded_body"],
                item["brief_json"],
            ),
        )
    await _conn.commit()

    saved = await get_push_brief_issue(normalized_issue_id)
    if not saved:
        raise RuntimeError("failed to save push brief issue")
    return saved


async def get_push_brief_issue(issue_id: str) -> Optional[Dict]:
    if not _conn:
        return None
    normalized_issue_id = str(issue_id or "").strip()
    if not normalized_issue_id:
        return None

    cursor = await _conn.execute(
        """
        SELECT id, issue_date, title, subtitle, footer, selection_count, created_by, created_at, updated_at
        FROM push_brief_issues
        WHERE id = ?
        LIMIT 1
        """,
        (normalized_issue_id,),
    )
    issue_row = await cursor.fetchone()
    if not issue_row:
        return None
    issue_columns = [description[0] for description in cursor.description]
    issue_item = dict(zip(issue_columns, issue_row))

    item_cursor = await _conn.execute(
        """
        SELECT id, issue_id, sort_index, article_id, source, headline, article_url, cover_image,
               warning, expanded_body, brief_json, created_at, updated_at
        FROM push_brief_issue_items
        WHERE issue_id = ?
        ORDER BY sort_index ASC, id ASC
        """,
        (normalized_issue_id,),
    )
    item_rows = await item_cursor.fetchall()
    item_columns = [description[0] for description in item_cursor.description]
    items = [dict(zip(item_columns, row)) for row in item_rows]

    return _build_issue_payload(issue_item, items)


async def get_push_brief_issue_by_id(issue_id: str) -> Optional[Dict]:
    return await get_push_brief_issue(issue_id)


async def get_latest_push_brief_issue() -> Optional[Dict]:
    if not _conn:
        return None

    cursor = await _conn.execute(
        """
        SELECT id
        FROM push_brief_issues
        ORDER BY issue_date DESC, updated_at DESC, created_at DESC
        LIMIT 1
        """
    )
    row = await cursor.fetchone()
    if not row:
        return None
    issue_id = str(row[0] or "").strip()
    if not issue_id:
        return None
    return await get_push_brief_issue(issue_id)


async def get_latest_push_brief_item_by_article_id(article_id: int) -> Optional[Dict]:
    if not _conn:
        return None

    try:
        normalized_article_id = int(article_id)
    except (TypeError, ValueError):
        return None

    if normalized_article_id <= 0:
        return None

    cursor = await _conn.execute(
        """
        SELECT
            i.id,
            i.issue_id,
            p.issue_date,
            p.title AS issue_title,
            p.subtitle AS issue_subtitle,
            p.footer AS issue_footer,
            i.sort_index,
            i.article_id,
            i.source,
            i.headline,
            i.article_url,
            i.cover_image,
            i.warning,
            i.expanded_body,
            i.brief_json,
            i.created_at,
            i.updated_at
        FROM push_brief_issue_items i
        JOIN push_brief_issues p ON p.id = i.issue_id
        WHERE i.article_id = ?
        ORDER BY p.issue_date DESC, p.updated_at DESC, p.created_at DESC, i.sort_index ASC, i.id DESC
        LIMIT 1
        """,
        (normalized_article_id,),
    )
    row = await cursor.fetchone()
    if not row:
        return None

    columns = [description[0] for description in cursor.description]
    item = dict(zip(columns, row))
    return {
        "id": item.get("id"),
        "issueId": item.get("issue_id"),
        "issueDate": item.get("issue_date"),
        "issueTitle": item.get("issue_title", ""),
        "issueSubtitle": item.get("issue_subtitle", ""),
        "issueFooter": item.get("issue_footer", ""),
        "sortIndex": int(item.get("sort_index") or 0),
        "newsId": int(item.get("article_id") or 0),
        "source": str(item.get("source") or "").strip(),
        "headline": str(item.get("headline") or "").strip(),
        "warning": str(item.get("warning") or "").strip(),
        "articleUrl": str(item.get("article_url") or "").strip(),
        "coverImage": str(item.get("cover_image") or "").strip(),
        "expandedBody": _deserialize_issue_body(item.get("expanded_body") or ""),
        "brief": _parse_issue_brief(item.get("brief_json") or ""),
        "createdAt": item.get("created_at"),
        "updatedAt": item.get("updated_at"),
    }


def _parse_json_array(text: str) -> List:
    raw = str(text or "").strip()
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return value if isinstance(value, list) else []


def _parse_json_dict(text: str) -> Dict:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _normalize_email_value(email: str) -> str:
    normalized = str(email or "").strip().lower()
    if not normalized or not _EMAIL_RE.fullmatch(normalized):
        raise ValueError("invalid email")
    return normalized


def _build_email_subscriber_payload(row: Dict) -> Dict:
    return {
        "id": int(row.get("id") or 0),
        "email": str(row.get("email") or "").strip().lower(),
        "userId": str(row.get("user_id") or "").strip(),
        "displayName": str(row.get("display_name") or "").strip(),
        "status": str(row.get("status") or "").strip() or "active",
        "source": str(row.get("source") or "").strip(),
        "unsubscribeToken": str(row.get("unsubscribe_token") or "").strip(),
        "verifiedAt": row.get("verified_at"),
        "unsubscribedAt": row.get("unsubscribed_at"),
        "lastSentAt": row.get("last_sent_at"),
        "createdAt": row.get("created_at"),
        "updatedAt": row.get("updated_at"),
    }


async def upsert_email_subscriber(
    email: str,
    *,
    user_id: str = "",
    display_name: str = "",
    source: str = "manual",
    status: str = "active",
    verified: bool = False,
) -> Dict:
    if not _conn:
        raise RuntimeError("database not initialized")

    normalized_email = _normalize_email_value(email)
    normalized_status = str(status or "active").strip().lower() or "active"
    if normalized_status not in {"active", "paused", "unsubscribed"}:
        raise ValueError("invalid subscriber status")

    existing_cursor = await _conn.execute(
        """
        SELECT id, unsubscribe_token
        FROM email_subscribers
        WHERE email = ?
        LIMIT 1
        """,
        (normalized_email,),
    )
    existing_row = await existing_cursor.fetchone()
    unsubscribe_token = (
        str(existing_row[1] or "").strip()
        if existing_row and str(existing_row[1] or "").strip()
        else secrets.token_urlsafe(24)
    )

    verified_at_sql = "CURRENT_TIMESTAMP" if verified else "verified_at"
    unsubscribed_at_sql = "CURRENT_TIMESTAMP" if normalized_status == "unsubscribed" else "NULL"
    await _conn.execute(
        f"""
        INSERT INTO email_subscribers (
            email, user_id, display_name, status, source, unsubscribe_token,
            verified_at, unsubscribed_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, {verified_at_sql}, {unsubscribed_at_sql}, CURRENT_TIMESTAMP)
        ON CONFLICT(email) DO UPDATE SET
            user_id = excluded.user_id,
            display_name = excluded.display_name,
            status = excluded.status,
            source = excluded.source,
            updated_at = CURRENT_TIMESTAMP,
            verified_at = CASE
                WHEN excluded.verified_at IS NOT NULL THEN excluded.verified_at
                ELSE email_subscribers.verified_at
            END,
            unsubscribed_at = CASE
                WHEN excluded.status = 'unsubscribed' THEN CURRENT_TIMESTAMP
                WHEN excluded.status = 'active' THEN NULL
                ELSE email_subscribers.unsubscribed_at
            END
        """,
        (
            normalized_email,
            str(user_id or "").strip(),
            str(display_name or "").strip(),
            normalized_status,
            str(source or "manual").strip(),
            unsubscribe_token,
        ),
    )
    await _conn.commit()

    cursor = await _conn.execute(
        """
        SELECT id, email, user_id, display_name, status, source, unsubscribe_token,
               verified_at, unsubscribed_at, last_sent_at, created_at, updated_at
        FROM email_subscribers
        WHERE email = ?
        LIMIT 1
        """,
        (normalized_email,),
    )
    row = await cursor.fetchone()
    if not row:
        raise RuntimeError("failed to save email subscriber")
    columns = [description[0] for description in cursor.description]
    return _build_email_subscriber_payload(dict(zip(columns, row)))


async def list_email_subscribers(active_only: bool = False) -> List[Dict]:
    if not _conn:
        return []

    sql = """
        SELECT id, email, user_id, display_name, status, source, unsubscribe_token,
               verified_at, unsubscribed_at, last_sent_at, created_at, updated_at
        FROM email_subscribers
    """
    params: tuple = ()
    if active_only:
        sql += " WHERE status = 'active'"
    sql += " ORDER BY updated_at DESC, id DESC"

    cursor = await _conn.execute(sql, params)
    rows = await cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    return [
        _build_email_subscriber_payload(dict(zip(columns, row)))
        for row in rows
    ]


async def unsubscribe_email_subscriber_by_token(token: str) -> Optional[Dict]:
    if not _conn:
        return None

    normalized_token = str(token or "").strip()
    if not normalized_token:
        return None

    await _conn.execute(
        """
        UPDATE email_subscribers
        SET status = 'unsubscribed',
            unsubscribed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE unsubscribe_token = ?
        """,
        (normalized_token,),
    )
    await _conn.commit()

    cursor = await _conn.execute(
        """
        SELECT id, email, user_id, display_name, status, source, unsubscribe_token,
               verified_at, unsubscribed_at, last_sent_at, created_at, updated_at
        FROM email_subscribers
        WHERE unsubscribe_token = ?
        LIMIT 1
        """,
        (normalized_token,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    columns = [description[0] for description in cursor.description]
    return _build_email_subscriber_payload(dict(zip(columns, row)))


async def touch_email_subscriber_sent_at(email: str) -> None:
    if not _conn:
        return

    normalized_email = str(email or "").strip().lower()
    if not normalized_email:
        return

    await _conn.execute(
        """
        UPDATE email_subscribers
        SET last_sent_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE email = ?
        """,
        (normalized_email,),
    )
    await _conn.commit()


async def create_email_publish_record(
    issue_id: str,
    *,
    subject: str = "",
    status: str = "",
    sent_count: int = 0,
    failed_count: int = 0,
    created_by: str = "",
    raw: Optional[Dict] = None,
) -> Dict:
    if not _conn:
        raise RuntimeError("database not initialized")

    normalized_issue_id = str(issue_id or "").strip()
    if not normalized_issue_id:
        raise ValueError("missing issue_id")

    await _conn.execute(
        """
        INSERT INTO email_publish_records (
            issue_id, subject, status, sent_count, failed_count, created_by, raw_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            normalized_issue_id,
            str(subject or "").strip(),
            str(status or "").strip(),
            int(sent_count or 0),
            int(failed_count or 0),
            str(created_by or "").strip(),
            json.dumps(raw or {}, ensure_ascii=False),
        ),
    )
    await _conn.commit()

    cursor = await _conn.execute(
        """
        SELECT id, issue_id, subject, status, sent_count, failed_count, created_by,
               raw_json, created_at, updated_at
        FROM email_publish_records
        WHERE rowid = last_insert_rowid()
        LIMIT 1
        """
    )
    row = await cursor.fetchone()
    if not row:
        raise RuntimeError("failed to create email publish record")
    columns = [description[0] for description in cursor.description]
    item = dict(zip(columns, row))
    return {
        "id": int(item.get("id") or 0),
        "issueId": str(item.get("issue_id") or "").strip(),
        "subject": str(item.get("subject") or "").strip(),
        "status": str(item.get("status") or "").strip(),
        "sentCount": int(item.get("sent_count") or 0),
        "failedCount": int(item.get("failed_count") or 0),
        "createdBy": str(item.get("created_by") or "").strip(),
        "raw": _parse_json_dict(item.get("raw_json") or ""),
        "createdAt": item.get("created_at"),
        "updatedAt": item.get("updated_at"),
    }


def _build_wechat_publish_payload(row: Dict) -> Dict:
    return {
        "id": int(row.get("id") or 0),
        "issueId": str(row.get("issue_id") or "").strip(),
        "draftMediaId": str(row.get("draft_media_id") or "").strip(),
        "publishId": str(row.get("publish_id") or "").strip(),
        "publishStatus": int(row.get("publish_status") or -1),
        "status": str(row.get("status") or "").strip(),
        "articleId": str(row.get("article_id") or "").strip(),
        "articleUrls": [
            str(item or "").strip()
            for item in _parse_json_array(row.get("article_urls_json") or "")
            if str(item or "").strip()
        ],
        "failIdx": [
            int(item)
            for item in _parse_json_array(row.get("fail_idx_json") or "")
            if str(item).strip().lstrip("-").isdigit()
        ],
        "errorMessage": str(row.get("error_message") or "").strip(),
        "createdBy": str(row.get("created_by") or "").strip(),
        "raw": _parse_json_dict(row.get("raw_json") or ""),
        "createdAt": row.get("created_at"),
        "updatedAt": row.get("updated_at"),
    }


async def create_wechat_publish_record(
    issue_id: str,
    *,
    created_by: str = "",
    draft_media_id: str = "",
    publish_id: str = "",
    publish_status: int = -1,
    status: str = "",
    article_id: str = "",
    article_urls: Optional[List[str]] = None,
    fail_idx: Optional[List[int]] = None,
    error_message: str = "",
    raw: Optional[Dict] = None,
) -> Dict:
    if not _conn:
        raise RuntimeError("database not initialized")

    normalized_issue_id = str(issue_id or "").strip()
    if not normalized_issue_id:
        raise ValueError("missing issue_id")

    cursor = await _conn.execute(
        """
        INSERT INTO wechat_publish_records (
            issue_id,
            draft_media_id,
            publish_id,
            publish_status,
            status,
            article_id,
            article_urls_json,
            fail_idx_json,
            error_message,
            created_by,
            raw_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            normalized_issue_id,
            str(draft_media_id or "").strip(),
            str(publish_id or "").strip(),
            int(publish_status),
            str(status or "").strip(),
            str(article_id or "").strip(),
            json.dumps(article_urls or [], ensure_ascii=False),
            json.dumps(fail_idx or [], ensure_ascii=False),
            str(error_message or "").strip(),
            str(created_by or "").strip(),
            json.dumps(raw or {}, ensure_ascii=False),
        ),
    )
    await _conn.commit()
    return await get_wechat_publish_record(int(cursor.lastrowid or 0))


async def update_wechat_publish_record(
    record_id: int,
    *,
    draft_media_id: Optional[str] = None,
    publish_id: Optional[str] = None,
    publish_status: Optional[int] = None,
    status: Optional[str] = None,
    article_id: Optional[str] = None,
    article_urls: Optional[List[str]] = None,
    fail_idx: Optional[List[int]] = None,
    error_message: Optional[str] = None,
    raw: Optional[Dict] = None,
) -> Optional[Dict]:
    if not _conn:
        raise RuntimeError("database not initialized")

    try:
        normalized_record_id = int(record_id)
    except (TypeError, ValueError):
        return None
    if normalized_record_id <= 0:
        return None

    sets = []
    params: List = []
    if draft_media_id is not None:
        sets.append("draft_media_id = ?")
        params.append(str(draft_media_id or "").strip())
    if publish_id is not None:
        sets.append("publish_id = ?")
        params.append(str(publish_id or "").strip())
    if publish_status is not None:
        sets.append("publish_status = ?")
        params.append(int(publish_status))
    if status is not None:
        sets.append("status = ?")
        params.append(str(status or "").strip())
    if article_id is not None:
        sets.append("article_id = ?")
        params.append(str(article_id or "").strip())
    if article_urls is not None:
        sets.append("article_urls_json = ?")
        params.append(json.dumps(article_urls or [], ensure_ascii=False))
    if fail_idx is not None:
        sets.append("fail_idx_json = ?")
        params.append(json.dumps(fail_idx or [], ensure_ascii=False))
    if error_message is not None:
        sets.append("error_message = ?")
        params.append(str(error_message or "").strip())
    if raw is not None:
        sets.append("raw_json = ?")
        params.append(json.dumps(raw or {}, ensure_ascii=False))
    if not sets:
        return await get_wechat_publish_record(normalized_record_id)

    sets.append("updated_at = CURRENT_TIMESTAMP")
    params.append(normalized_record_id)
    await _conn.execute(
        f"""
        UPDATE wechat_publish_records
        SET {", ".join(sets)}
        WHERE id = ?
        """,
        tuple(params),
    )
    await _conn.commit()
    return await get_wechat_publish_record(normalized_record_id)


async def get_wechat_publish_record(record_id: int) -> Optional[Dict]:
    if not _conn:
        return None

    try:
        normalized_record_id = int(record_id)
    except (TypeError, ValueError):
        return None
    if normalized_record_id <= 0:
        return None

    cursor = await _conn.execute(
        """
        SELECT
            id,
            issue_id,
            draft_media_id,
            publish_id,
            publish_status,
            status,
            article_id,
            article_urls_json,
            fail_idx_json,
            error_message,
            created_by,
            raw_json,
            created_at,
            updated_at
        FROM wechat_publish_records
        WHERE id = ?
        LIMIT 1
        """,
        (normalized_record_id,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    columns = [description[0] for description in cursor.description]
    return _build_wechat_publish_payload(dict(zip(columns, row)))


async def get_wechat_publish_record_by_publish_id(publish_id: str) -> Optional[Dict]:
    if not _conn:
        return None

    normalized_publish_id = str(publish_id or "").strip()
    if not normalized_publish_id:
        return None

    cursor = await _conn.execute(
        """
        SELECT
            id,
            issue_id,
            draft_media_id,
            publish_id,
            publish_status,
            status,
            article_id,
            article_urls_json,
            fail_idx_json,
            error_message,
            created_by,
            raw_json,
            created_at,
            updated_at
        FROM wechat_publish_records
        WHERE publish_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (normalized_publish_id,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    columns = [description[0] for description in cursor.description]
    return _build_wechat_publish_payload(dict(zip(columns, row)))
