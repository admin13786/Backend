import http.client
import json
import logging
import os
import re
import shlex
import shutil
import socket
import sqlite3
import subprocess
import threading
import time
import uuid
from hashlib import sha256
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import quote, urlparse

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from pydantic import BaseModel, Field


def load_local_env() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_local_env()


def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


DATA_ROOT = Path(os.getenv("AGENT_DATA_ROOT", "./data")).resolve()
AGENT_DATA_HOST_ROOT = os.getenv("AGENT_DATA_HOST_ROOT", "").strip()
SESSIONS_ROOT = DATA_ROOT / "agent-sessions"
DATABASE_PATH = DATA_ROOT / "app.db"
STATIC_ROOT = Path(__file__).resolve().parent / "static"

CLAUDE_DOCKER_IMAGE = os.getenv("CLAUDE_DOCKER_IMAGE", "claude-runtime:latest")
CLAUDE_TIMEOUT_SECONDS = int(os.getenv("CLAUDE_TIMEOUT_SECONDS", "900"))
CLAUDE_MEMORY = os.getenv("CLAUDE_MEMORY", "2g")
CLAUDE_CPUS = os.getenv("CLAUDE_CPUS", "1")
CLAUDE_IDLE_TIMEOUT_SECONDS = int(os.getenv("CLAUDE_IDLE_TIMEOUT_SECONDS", "120"))
CLAUDE_IDLE_SWEEP_SECONDS = int(os.getenv("CLAUDE_IDLE_SWEEP_SECONDS", "60"))
APP_RUNTIME_IMAGE = env_or_default("APP_RUNTIME_IMAGE", "node:20-alpine")
APP_RUNTIME_NPM_REGISTRY = env_or_default("APP_RUNTIME_NPM_REGISTRY", "https://registry.npmmirror.com")
APP_RUNTIME_MEMORY = env_or_default("APP_RUNTIME_MEMORY", "2g")
APP_RUNTIME_CPUS = env_or_default("APP_RUNTIME_CPUS", "1")
APP_RUNTIME_INTERNAL_PORT = int(env_or_default("APP_RUNTIME_INTERNAL_PORT", "3000"))
APP_RUNTIME_START_TIMEOUT_SECONDS = int(env_or_default("APP_RUNTIME_START_TIMEOUT_SECONDS", "30"))
DEFAULT_USER_ID = os.getenv("DEFAULT_USER_ID", "demo-user")
DEFAULT_CONTAINER_UID = "1000" if os.getuid() == 0 else str(os.getuid())
DEFAULT_CONTAINER_GID = "1000" if os.getgid() == 0 else str(os.getgid())
CONTAINER_UID = env_or_default("CLAUDE_CONTAINER_UID", DEFAULT_CONTAINER_UID)
CONTAINER_GID = env_or_default("CLAUDE_CONTAINER_GID", DEFAULT_CONTAINER_GID)
CONTAINER_HOME = env_or_default("CLAUDE_CONTAINER_HOME", "/home/agent")
DEFAULT_RUNTIME_PROFILE = env_or_default("DEFAULT_RUNTIME_PROFILE", "aliyun")
DEFAULT_CLAUDE_MODEL = env_or_default("DEFAULT_CLAUDE_MODEL", "sonnet")
ALIYUN_ANTHROPIC_BASE_URL = env_or_default(
    "ALIYUN_ANTHROPIC_BASE_URL",
    "https://dashscope.aliyuncs.com/apps/anthropic",
)
ALIYUN_ANTHROPIC_API_KEY = env_or_default("ALIYUN_ANTHROPIC_API_KEY", "")
ALIYUN_ANTHROPIC_AUTH_TOKEN = env_or_default("ALIYUN_ANTHROPIC_AUTH_TOKEN", "")
ALIYUN_ANTHROPIC_MODEL = env_or_default("ALIYUN_ANTHROPIC_MODEL", "qwen3-coder-next")
DEFAULT_APPEND_SYSTEM_PROMPT = (
    "When you create or modify files, verify the result before claiming success. "
    "Re-read the changed files and only say a file was updated if the workspace contents actually reflect the change. "
    "If no file was changed, say so explicitly. "
    "When the user request is already specific enough to act on, do not ask follow-up questions; "
    "make the change directly in the workspace."
)
CLAUDE_ENV_VARS = [
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_CUSTOM_HEADERS",
]
WORKSPACE_TREE_IGNORED_DIRS = {
    ".agentdo",
    ".next",
    ".nuxt",
    ".pytest_cache",
    "__pycache__",
    "coverage",
    "dist",
    "build",
    "node_modules",
}
WORKSPACE_FILE_MAX_BYTES = 256 * 1024
PLAYWRIGHT_BROWSER = env_or_default("PLAYWRIGHT_BROWSER", "chromium")
PLAYWRIGHT_VIEWPORT_WIDTH = int(env_or_default("PLAYWRIGHT_VIEWPORT_WIDTH", "1440"))
PLAYWRIGHT_VIEWPORT_HEIGHT = int(env_or_default("PLAYWRIGHT_VIEWPORT_HEIGHT", "960"))
PLAYWRIGHT_NAV_TIMEOUT_MS = int(env_or_default("PLAYWRIGHT_NAV_TIMEOUT_MS", "45000"))
PLAYWRIGHT_SETTLE_MS = int(env_or_default("PLAYWRIGHT_SETTLE_MS", "1500"))
PLAYWRIGHT_ACTION_SETTLE_MS = int(env_or_default("PLAYWRIGHT_ACTION_SETTLE_MS", "1200"))
PLAYWRIGHT_MAX_CONSOLE_ERRORS = int(env_or_default("PLAYWRIGHT_MAX_CONSOLE_ERRORS", "0"))
PLAYWRIGHT_ACCEPTANCE_INTERNAL_BASE_URL = env_or_default(
    "PLAYWRIGHT_ACCEPTANCE_INTERNAL_BASE_URL",
    "http://127.0.0.1:8000",
).rstrip("/")
DEFAULT_AUTO_REPAIR_ROUNDS = int(env_or_default("DEFAULT_AUTO_REPAIR_ROUNDS", "1"))
MAX_AUTO_REPAIR_ROUNDS = int(env_or_default("MAX_AUTO_REPAIR_ROUNDS", "2"))
STREAM_KEEPALIVE_INTERVAL_SECONDS = float(env_or_default("STREAM_KEEPALIVE_INTERVAL_SECONDS", "10"))
APP_RUNTIME_IDLE_TIMEOUT_SECONDS = int(env_or_default("APP_RUNTIME_IDLE_TIMEOUT_SECONDS", "3600"))
APP_RUNTIME_IDLE_SWEEP_SECONDS = int(env_or_default("APP_RUNTIME_IDLE_SWEEP_SECONDS", "120"))
AGENTDO_ORPHAN_SWEEP_SECONDS = int(env_or_default("AGENTDO_ORPHAN_SWEEP_SECONDS", "300"))
AGENT_SESSION_RETENTION_SECONDS = int(env_or_default("AGENT_SESSION_RETENTION_SECONDS", "0"))
AGENT_SESSION_RETENTION_SWEEP_SECONDS = int(env_or_default("AGENT_SESSION_RETENTION_SWEEP_SECONDS", "1800"))
LOGGER = logging.getLogger("uvicorn.error")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_timestamp(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None


def format_duration(seconds: int) -> str:
    if seconds <= 0:
        return "0 seconds"
    if seconds % 86400 == 0:
        days = seconds // 86400
        return f"{days} day" if days == 1 else f"{days} days"
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return f"{hours} hour" if hours == 1 else f"{hours} hours"
    if seconds % 60 == 0:
        minutes = seconds // 60
        return f"{minutes} minute" if minutes == 1 else f"{minutes} minutes"
    return f"{seconds} seconds"


def nested_docker_host_path(path: Path) -> Path:
    resolved = path.resolve()
    if not AGENT_DATA_HOST_ROOT:
        return resolved
    try:
        relative = resolved.relative_to(DATA_ROOT)
    except ValueError:
        return resolved
    return (Path(AGENT_DATA_HOST_ROOT) / relative).resolve()


def ensure_data_dirs() -> None:
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    SESSIONS_ROOT.mkdir(parents=True, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                title TEXT,
                workspace_path TEXT NOT NULL,
                home_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_active_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                exit_code INTEGER,
                duration_ms INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY(session_id) REFERENCES sessions(id)
            );

            CREATE TABLE IF NOT EXISTS runtimes (
                session_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                entry_file TEXT,
                container_name TEXT,
                host_port INTEGER,
                internal_port INTEGER,
                install_command TEXT,
                start_command TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(session_id) REFERENCES sessions(id)
            );

            CREATE TABLE IF NOT EXISTS claude_runtimes (
                session_id TEXT PRIMARY KEY,
                container_name TEXT NOT NULL,
                status TEXT NOT NULL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                FOREIGN KEY(session_id) REFERENCES sessions(id)
            );
            """
        )


def row_to_dict(row: sqlite3.Row) -> dict:
    return {key: row[key] for key in row.keys()}


def fetch_session(session_id: str) -> dict:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return row_to_dict(row)


def has_assistant_reply(session_id: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM messages WHERE session_id = ? AND role = 'assistant' LIMIT 1",
            (session_id,),
        ).fetchone()
    return row is not None


def fetch_runtime_record(session_id: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM runtimes WHERE session_id = ?",
            (session_id,),
        ).fetchone()
    if row is None:
        return None
    return row_to_dict(row)


def fetch_claude_runtime_record(session_id: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM claude_runtimes WHERE session_id = ?",
            (session_id,),
        ).fetchone()
    if row is None:
        return None
    return row_to_dict(row)


def upsert_runtime_record(
    session_id: str,
    mode: str,
    status: str,
    entry_file: str | None = None,
    container_name: str | None = None,
    host_port: int | None = None,
    internal_port: int | None = None,
    install_command: str | None = None,
    start_command: str | None = None,
    last_error: str | None = None,
) -> dict:
    existing = fetch_runtime_record(session_id)
    now = utc_now()
    payload = {
        "session_id": session_id,
        "mode": mode,
        "status": status,
        "entry_file": entry_file,
        "container_name": container_name,
        "host_port": host_port,
        "internal_port": internal_port,
        "install_command": install_command,
        "start_command": start_command,
        "last_error": last_error,
        "created_at": existing["created_at"] if existing else now,
        "updated_at": now,
    }
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO runtimes (
                session_id, mode, status, entry_file, container_name, host_port,
                internal_port, install_command, start_command, last_error, created_at, updated_at
            )
            VALUES (
                :session_id, :mode, :status, :entry_file, :container_name, :host_port,
                :internal_port, :install_command, :start_command, :last_error, :created_at, :updated_at
            )
            ON CONFLICT(session_id) DO UPDATE SET
                mode=excluded.mode,
                status=excluded.status,
                entry_file=excluded.entry_file,
                container_name=excluded.container_name,
                host_port=excluded.host_port,
                internal_port=excluded.internal_port,
                install_command=excluded.install_command,
                start_command=excluded.start_command,
                last_error=excluded.last_error,
                updated_at=excluded.updated_at
            """,
            payload,
        )
        conn.commit()
    return fetch_runtime_record(session_id) or payload


def upsert_claude_runtime_record(
    session_id: str,
    container_name: str,
    status: str,
    last_error: str | None = None,
    last_used_at: str | None = None,
) -> dict:
    existing = fetch_claude_runtime_record(session_id)
    now = utc_now()
    payload = {
        "session_id": session_id,
        "container_name": container_name,
        "status": status,
        "last_error": last_error,
        "created_at": existing["created_at"] if existing else now,
        "updated_at": now,
        "last_used_at": last_used_at or (existing["last_used_at"] if existing else now),
    }
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO claude_runtimes (
                session_id, container_name, status, last_error, created_at, updated_at, last_used_at
            )
            VALUES (
                :session_id, :container_name, :status, :last_error, :created_at, :updated_at, :last_used_at
            )
            ON CONFLICT(session_id) DO UPDATE SET
                container_name=excluded.container_name,
                status=excluded.status,
                last_error=excluded.last_error,
                updated_at=excluded.updated_at,
                last_used_at=excluded.last_used_at
            """,
            payload,
        )
        conn.commit()
    return fetch_claude_runtime_record(session_id) or payload


def insert_message(
    session_id: str,
    role: str,
    content: str,
    exit_code: int | None = None,
    duration_ms: int | None = None,
    created_at: str | None = None,
) -> None:
    timestamp = created_at or utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO messages (id, session_id, role, content, exit_code, duration_ms, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                session_id,
                role,
                content,
                exit_code,
                duration_ms,
                timestamp,
            ),
        )
        conn.execute(
            "UPDATE sessions SET last_active_at = ? WHERE id = ?",
            (timestamp, session_id),
        )
        conn.commit()


def build_session_payload(session_id: str) -> dict:
    claude_runtime = refresh_claude_runtime_record(fetch_claude_runtime_record(session_id))
    session = fetch_session(session_id)
    return {
        **session,
        "claude_runtime": claude_runtime,
    }


def delete_session_data(session_id: str) -> dict:
    session = fetch_session(session_id)
    runtime_record = fetch_runtime_record(session_id)
    container_name = runtime_record["container_name"] if runtime_record and runtime_record.get("container_name") else runtime_container_name(session_id)
    remove_runtime_container(container_name)
    claude_record = fetch_claude_runtime_record(session_id)
    claude_container = claude_record["container_name"] if claude_record and claude_record.get("container_name") else claude_container_name(session_id)
    remove_runtime_container(claude_container)

    session_root = Path(session["workspace_path"]).resolve().parent

    with get_conn() as conn:
        conn.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
        conn.execute("DELETE FROM runtimes WHERE session_id = ?", (session_id,))
        conn.execute("DELETE FROM claude_runtimes WHERE session_id = ?", (session_id,))
        conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
        conn.commit()

    shutil.rmtree(session_root, ignore_errors=True)
    return session


def stop_claude_runtime_for_session(session: dict, reason: str = "Claude runtime stopped.") -> dict:
    session_id = session["id"]
    record = refresh_claude_runtime_record(fetch_claude_runtime_record(session_id))
    if not record or not record.get("container_name"):
        return {
            "session_id": session_id,
            "status": "not_started",
            "container_name": None,
            "stopped": False,
        }

    container_name = record["container_name"]
    remove_runtime_container(container_name)
    updated = upsert_claude_runtime_record(
        session_id=session_id,
        container_name=container_name,
        status="stopped",
        last_error=reason,
        last_used_at=record.get("last_used_at") or utc_now(),
    )
    return {
        "session_id": session_id,
        "status": updated.get("status"),
        "container_name": container_name,
        "stopped": True,
        "last_error": updated.get("last_error"),
    }


def snapshot_workspace(workspace_path: Path) -> dict[str, str]:
    snapshot: dict[str, str] = {}
    if not workspace_path.exists():
        return snapshot

    for path in sorted(workspace_path.rglob("*")):
        if not path.is_file():
            continue
        relpath = str(path.relative_to(workspace_path))
        digest = sha256(path.read_bytes()).hexdigest()
        snapshot[relpath] = digest
    return snapshot


def workspace_has_files(workspace_path: Path) -> bool:
    if not workspace_path.exists():
        return False
    for path in workspace_path.rglob("*"):
        if path.is_file():
            return True
    return False


def diff_workspace(before: dict[str, str], after: dict[str, str]) -> dict[str, list[str]]:
    before_keys = set(before)
    after_keys = set(after)

    added = sorted(after_keys - before_keys)
    deleted = sorted(before_keys - after_keys)
    modified = sorted(path for path in before_keys & after_keys if before[path] != after[path])
    changed_files = added + modified + deleted

    return {
        "added": added,
        "modified": modified,
        "deleted": deleted,
        "changed_files": changed_files,
    }


def should_flag_missing_changes(prompt: str, output: str) -> bool:
    text = f"{prompt}\n{output}".lower()
    indicators = [
        "创建",
        "修改",
        "改名",
        "重命名",
        "写入",
        "保存",
        "文件",
        "html",
        "readme",
        "created",
        "updated",
        "modified",
        "renamed",
        "saved",
        "wrote",
        "file",
    ]
    return any(token in text for token in indicators)


def maybe_annotate_output(prompt: str, output: str, workspace_diff: dict[str, list[str]]) -> str:
    if workspace_diff["changed_files"]:
        changed = ", ".join(workspace_diff["changed_files"][:10])
        suffix = f"\n\n[Backend note: Workspace changed files: {changed}]"
        return f"{output}{suffix}"

    if should_flag_missing_changes(prompt, output):
        suffix = "\n\n[Backend note: No workspace file changes were detected during this run.]"
        return f"{output}{suffix}"

    return output


def run_command(
    command: list[str],
    timeout: int | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=f"Missing runtime dependency: {exc}") from exc

    if check and completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "Command failed"
        raise HTTPException(status_code=500, detail=detail)
    return completed


def normalize_shell_command(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, list):
        return shlex.join([str(part) for part in value])
    return None


def load_json_file(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if isinstance(data, dict):
        return data
    return None


def detect_static_entry(workspace_path: Path) -> str | None:
    direct_index = workspace_path / "index.html"
    if direct_index.exists():
        return "index.html"

    html_files = sorted(
        str(path.relative_to(workspace_path))
        for path in workspace_path.rglob("*.html")
        if path.is_file() and ".agentdo" not in path.parts
    )
    if len(html_files) == 1:
        return html_files[0]
    return None


def extract_html_document(text: str) -> str | None:
    stripped = text.strip()
    if not stripped:
        return None

    candidates: list[str] = []
    for match in re.finditer(r"```[^\n]*\n([\s\S]*?)```", stripped):
        block = match.group(1).strip()
        if block:
            candidates.append(block)
    candidates.append(stripped)

    for candidate in candidates:
        start_match = re.search(r"<!doctype\s+html\b|<html\b", candidate, flags=re.IGNORECASE)
        if not start_match:
            continue
        html = candidate[start_match.start():].strip()
        end_match = re.search(r"</html>", html, flags=re.IGNORECASE)
        if end_match:
            html = html[: end_match.end()].strip()
        lowered = html.lower()
        if "<body" not in lowered and "<head" not in lowered:
            continue
        return html

    return None


def prefers_single_html_materialization(prompt: str) -> bool:
    lowered = str(prompt or "").lower()
    if not lowered:
        return False

    vite_markers = (
        "vite multi-file project",
        "build the frontend as a vite multi-file project",
        "/workspace/package.json",
        "/workspace/src/main.js",
        "usable dev or start script",
    )
    if any(marker in lowered for marker in vite_markers):
        return False

    single_html_markers = (
        "single /workspace/index.html",
        "single html",
        "inline css/js",
        "/workspace/index.html must exist",
        "single html project",
    )
    return any(marker in lowered for marker in single_html_markers)


def maybe_materialize_single_html_output(
    prompt: str,
    workspace_path: Path,
    output: str,
    workspace_diff: dict[str, list[str]],
) -> str | None:
    if not prefers_single_html_materialization(prompt):
        return None
    if workspace_diff["changed_files"]:
        return None
    if detect_static_entry(workspace_path):
        return None

    html = extract_html_document(output)
    if not html:
        return None

    target = workspace_path / "index.html"
    target.write_text(html, encoding="utf-8")
    LOGGER.info("Materialized Claude HTML output to %s", target)
    return "index.html"


def detect_runtime_spec(workspace_path: Path) -> dict:
    manifest = load_json_file(workspace_path / ".agentdo" / "project.json") or {}

    if manifest.get("start"):
        default_install = f"npm config set registry {shlex.quote(APP_RUNTIME_NPM_REGISTRY)} && npm install"
        return {
            "mode": "node",
            "entry_file": detect_static_entry(workspace_path),
            "install_command": normalize_shell_command(manifest.get("install")) or default_install,
            "start_command": normalize_shell_command(manifest.get("start")),
            "internal_port": int(manifest.get("port", APP_RUNTIME_INTERNAL_PORT)),
        }

    if manifest.get("entry"):
        entry_file = str(manifest["entry"])
        if (workspace_path / entry_file).is_file():
            return {
                "mode": "static",
                "entry_file": entry_file,
                "install_command": normalize_shell_command(manifest.get("install")),
                "start_command": normalize_shell_command(manifest.get("start")),
                "internal_port": int(manifest.get("port", APP_RUNTIME_INTERNAL_PORT)),
            }

    package_json = load_json_file(workspace_path / "package.json")
    if package_json:
        scripts = package_json.get("scripts") or {}
        internal_port = APP_RUNTIME_INTERNAL_PORT
        install_command = f"npm config set registry {shlex.quote(APP_RUNTIME_NPM_REGISTRY)} && npm install"
        start_command = None

        if "dev" in scripts:
            start_command = f"npm run dev -- --host 0.0.0.0 --port {internal_port}"
        elif "start" in scripts:
            start_command = "npm run start"

        return {
            "mode": "node",
            "entry_file": detect_static_entry(workspace_path),
            "install_command": install_command,
            "start_command": start_command,
            "internal_port": internal_port,
        }

    entry_file = detect_static_entry(workspace_path)
    if entry_file:
        return {
            "mode": "static",
            "entry_file": entry_file,
            "install_command": None,
            "start_command": None,
            "internal_port": None,
        }

    return {
        "mode": "unknown",
        "entry_file": None,
        "install_command": None,
        "start_command": None,
        "internal_port": None,
    }


def runtime_container_name(session_id: str) -> str:
    return f"agentdo-runtime-{session_id}"


def claude_container_name(session_id: str) -> str:
    return f"agentdo-claude-{session_id}"


def remove_runtime_container(container_name: str) -> None:
    run_command(["docker", "rm", "-f", container_name], check=False)


def list_agentdo_container_names() -> list[str]:
    result = run_command(["docker", "ps", "-a", "--format", "{{.Names}}"], check=False)
    if result.returncode != 0:
        return []
    return [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip().startswith("agentdo-")
    ]


def read_runtime_logs(container_name: str, lines: int = 80) -> str:
    result = run_command(
        ["docker", "logs", "--tail", str(lines), container_name],
        check=False,
    )
    return (result.stdout or result.stderr or "").strip()


def inspect_container(container_name: str) -> dict | None:
    result = run_command(["docker", "inspect", container_name], check=False)
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    if not data:
        return None
    return data[0]


def extract_host_port(inspect_data: dict, internal_port: int) -> int | None:
    ports = inspect_data.get("NetworkSettings", {}).get("Ports", {})
    bindings = ports.get(f"{internal_port}/tcp")
    if not bindings:
        return None
    host_port = bindings[0].get("HostPort")
    if not host_port:
        return None
    return int(host_port)


def wait_for_port(host: str, port: int, timeout_seconds: int) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def docker_host_gateway() -> str:
    override = os.environ.get("DOCKER_HOST_GATEWAY")
    if override:
        return override

    if not Path("/.dockerenv").exists():
        return "127.0.0.1"

    try:
        socket.gethostbyname("host.docker.internal")
        return "host.docker.internal"
    except OSError:
        pass

    try:
        with open("/proc/net/route", "r", encoding="utf-8") as handle:
            next(handle, None)
            for line in handle:
                fields = line.strip().split()
                if len(fields) < 3 or fields[1] != "00000000":
                    continue
                gateway_hex = fields[2]
                gateway_raw = bytes.fromhex(gateway_hex)
                return socket.inet_ntoa(gateway_raw[::-1])
    except OSError:
        pass

    return "127.0.0.1"


def wait_for_runtime_ready(container_name: str, internal_port: int, timeout_seconds: int) -> int | None:
    deadline = time.time() + timeout_seconds
    runtime_host = docker_host_gateway()
    while time.time() < deadline:
        inspect_data = inspect_container(container_name)
        if inspect_data is None:
            return None

        state = inspect_data.get("State", {})
        if not state.get("Running"):
            return None

        host_port = extract_host_port(inspect_data, internal_port)
        if host_port and wait_for_port(runtime_host, host_port, 1):
            return host_port

        time.sleep(0.5)
    return None


def refresh_runtime_record(record: dict | None) -> dict | None:
    if not record or record["mode"] != "node" or not record.get("container_name"):
        return record

    inspect_data = inspect_container(record["container_name"])
    if inspect_data is None:
        if record["status"] in {"running", "starting"}:
            return upsert_runtime_record(
                session_id=record["session_id"],
                mode=record["mode"],
                status="stopped",
                entry_file=record.get("entry_file"),
                container_name=record.get("container_name"),
                host_port=None,
                internal_port=record.get("internal_port"),
                install_command=record.get("install_command"),
                start_command=record.get("start_command"),
                last_error="Runtime container not found.",
            )
        return record

    state = inspect_data.get("State", {})
    running = bool(state.get("Running"))
    host_port = extract_host_port(inspect_data, int(record.get("internal_port") or APP_RUNTIME_INTERNAL_PORT))

    if running:
        if record["status"] != "running" or record.get("host_port") != host_port:
            return upsert_runtime_record(
                session_id=record["session_id"],
                mode=record["mode"],
                status="running",
                entry_file=record.get("entry_file"),
                container_name=record.get("container_name"),
                host_port=host_port,
                internal_port=record.get("internal_port"),
                install_command=record.get("install_command"),
                start_command=record.get("start_command"),
                last_error=None,
            )
        return record

    final_status = "stopped" if record["status"] == "stopped" else "failed"
    last_error = read_runtime_logs(record["container_name"]) or record.get("last_error")
    return upsert_runtime_record(
        session_id=record["session_id"],
        mode=record["mode"],
        status=final_status,
        entry_file=record.get("entry_file"),
        container_name=record.get("container_name"),
        host_port=None,
        internal_port=record.get("internal_port"),
        install_command=record.get("install_command"),
        start_command=record.get("start_command"),
        last_error=last_error,
    )


def refresh_claude_runtime_record(record: dict | None) -> dict | None:
    if not record or not record.get("container_name"):
        return record

    inspect_data = inspect_container(record["container_name"])
    if inspect_data is None:
        if record["status"] in {"running", "starting"}:
            return upsert_claude_runtime_record(
                session_id=record["session_id"],
                container_name=record["container_name"],
                status="stopped",
                last_error="Claude container not found.",
                last_used_at=record.get("last_used_at"),
            )
        return record

    state = inspect_data.get("State", {})
    if state.get("Running"):
        if record["status"] not in {"running", "busy"}:
            return upsert_claude_runtime_record(
                session_id=record["session_id"],
                container_name=record["container_name"],
                status="running",
                last_error=None,
                last_used_at=record.get("last_used_at"),
            )
        return record

    last_error = read_runtime_logs(record["container_name"]) or record.get("last_error")
    return upsert_claude_runtime_record(
        session_id=record["session_id"],
        container_name=record["container_name"],
        status="failed" if record["status"] != "stopped" else "stopped",
        last_error=last_error,
        last_used_at=record.get("last_used_at"),
    )


def build_runtime_payload(session: dict) -> dict:
    workspace_path = Path(session["workspace_path"]).resolve()
    spec = detect_runtime_spec(workspace_path)
    record = refresh_runtime_record(fetch_runtime_record(session["id"]))

    mode = spec["mode"]
    status = "not_available"
    can_start = False
    can_preview = False
    preview_url = None
    last_error = None
    host_port = None

    if mode == "static":
        status = "ready" if spec.get("entry_file") else "not_available"
        can_preview = bool(spec.get("entry_file"))
        preview_url = (
            f"/sessions/{session['id']}/preview/{quote(spec['entry_file'], safe='/')}"
            if can_preview
            else None
        )
    elif mode == "node":
        can_start = True
        if spec.get("start_command") is None:
            status = "not_configured"
            last_error = "检测到了 package.json，但没有可识别的 dev/start 脚本。"
        else:
            status = "stopped"
            if record:
                status = record["status"]
                last_error = record.get("last_error")
                host_port = record.get("host_port")
                if status == "running":
                    can_preview = True
                    preview_url = f"/sessions/{session['id']}/preview/"
    else:
        last_error = "当前 workspace 中没有可预览的静态页面，也没有可运行的 Node 项目。"

    return {
        "session_id": session["id"],
        "mode": mode,
        "status": status,
        "entry_file": spec.get("entry_file"),
        "preview_url": preview_url,
        "can_preview": can_preview,
        "can_start": can_start,
        "host_port": host_port,
        "container_name": record.get("container_name") if record else None,
        "install_command": spec.get("install_command") or (record.get("install_command") if record else None),
        "start_command": spec.get("start_command") or (record.get("start_command") if record else None),
        "internal_port": spec.get("internal_port") or (record.get("internal_port") if record else None),
        "last_error": last_error,
    }


def touch_runtime_record(record: dict | None) -> dict | None:
    if not record:
        return None
    return upsert_runtime_record(
        session_id=record["session_id"],
        mode=record["mode"],
        status=record["status"],
        entry_file=record.get("entry_file"),
        container_name=record.get("container_name"),
        host_port=record.get("host_port"),
        internal_port=record.get("internal_port"),
        install_command=record.get("install_command"),
        start_command=record.get("start_command"),
        last_error=record.get("last_error"),
    )


def ensure_claude_runtime_for_session(session: dict) -> dict:
    workspace_path = Path(session["workspace_path"]).resolve()
    home_path = Path(session["home_path"]).resolve()
    workspace_mount_path = nested_docker_host_path(workspace_path)
    home_mount_path = nested_docker_host_path(home_path)
    container_name = claude_container_name(session["id"])

    record = refresh_claude_runtime_record(fetch_claude_runtime_record(session["id"]))
    if record and record.get("container_name") == container_name and record["status"] == "running":
        return record

    remove_runtime_container(container_name)
    upsert_claude_runtime_record(
        session_id=session["id"],
        container_name=container_name,
        status="starting",
        last_error=None,
    )

    completed = run_command(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--memory",
            CLAUDE_MEMORY,
            "--cpus",
            CLAUDE_CPUS,
            "--user",
            f"{CONTAINER_UID}:{CONTAINER_GID}",
            "-e",
            f"HOME={CONTAINER_HOME}",
            "-v",
            f"{workspace_mount_path}:/workspace",
            "-v",
            f"{home_mount_path}:{CONTAINER_HOME}",
            "-w",
            "/workspace",
            CLAUDE_DOCKER_IMAGE,
            "tail",
            "-f",
            "/dev/null",
        ],
        timeout=60,
    )
    started_name = completed.stdout.strip() or container_name
    deadline = time.time() + 20
    while time.time() < deadline:
        inspect_data = inspect_container(container_name)
        if inspect_data and inspect_data.get("State", {}).get("Running"):
            return upsert_claude_runtime_record(
                session_id=session["id"],
                container_name=container_name,
                status="running",
                last_error=None,
                last_used_at=utc_now(),
            )
        time.sleep(0.5)

    last_error = read_runtime_logs(container_name) or f"Claude container {started_name} did not become ready in time."
    remove_runtime_container(container_name)
    return upsert_claude_runtime_record(
        session_id=session["id"],
        container_name=container_name,
        status="failed",
        last_error=last_error,
        last_used_at=utc_now(),
    )


def build_claude_exec_command(
    session: dict,
    prompt: str,
    model: str,
    max_turns: int,
    append_system_prompt: str | None,
    runtime_profile: str,
) -> list[str]:
    claude_env = validate_runtime_env(runtime_profile)
    runtime = ensure_claude_runtime_for_session(session)
    if runtime["status"] != "running":
        raise HTTPException(status_code=500, detail=runtime.get("last_error") or "Claude container is not running.")

    env_args = ["-e", f"HOME={CONTAINER_HOME}"]
    for name, value in claude_env.items():
        env_args.extend(["-e", f"{name}={value}"])

    command = [
        "docker",
        "exec",
        *env_args,
        "-w",
        "/workspace",
        runtime["container_name"],
        "claude",
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--permission-mode",
        "bypassPermissions",
        "--disallowedTools",
        "AskUserQuestion",
        "--model",
        model,
        "--max-turns",
        str(max_turns),
    ]

    final_system_prompt = DEFAULT_APPEND_SYSTEM_PROMPT
    if append_system_prompt:
        final_system_prompt = f"{DEFAULT_APPEND_SYSTEM_PROMPT}\n\n{append_system_prompt}"
    command.extend(["--append-system-prompt", final_system_prompt])

    if has_assistant_reply(session["id"]):
        command.append("-c")

    command.append(prompt)
    upsert_claude_runtime_record(
        session_id=session["id"],
        container_name=runtime["container_name"],
        status="busy",
        last_error=None,
        last_used_at=utc_now(),
    )
    return command


def start_runtime_for_session(session: dict) -> dict:
    workspace_path = Path(session["workspace_path"]).resolve()
    home_path = Path(session["home_path"]).resolve()
    workspace_mount_path = nested_docker_host_path(workspace_path)
    home_mount_path = nested_docker_host_path(home_path)
    spec = detect_runtime_spec(workspace_path)

    if spec["mode"] == "static":
        upsert_runtime_record(
            session_id=session["id"],
            mode="static",
            status="ready",
            entry_file=spec.get("entry_file"),
        )
        return build_runtime_payload(session)

    if spec["mode"] != "node":
        raise HTTPException(status_code=400, detail="当前 session 没有可在线运行的项目。")

    if not spec.get("start_command"):
        raise HTTPException(status_code=400, detail="检测到了 package.json，但没有可运行的 dev/start 脚本。")

    container_name = runtime_container_name(session["id"])
    remove_runtime_container(container_name)
    upsert_runtime_record(
        session_id=session["id"],
        mode="node",
        status="starting",
        entry_file=spec.get("entry_file"),
        container_name=container_name,
        host_port=None,
        internal_port=spec.get("internal_port"),
        install_command=spec.get("install_command"),
        start_command=spec.get("start_command"),
        last_error=None,
    )

    install_command = spec.get("install_command")
    if install_command:
        boot_command = f"set -e; if [ ! -d node_modules ]; then {install_command}; fi; {spec['start_command']}"
    else:
        boot_command = spec["start_command"]

    run_command(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--memory",
            APP_RUNTIME_MEMORY,
            "--cpus",
            APP_RUNTIME_CPUS,
            "--user",
            f"{CONTAINER_UID}:{CONTAINER_GID}",
            "-e",
            f"HOME={CONTAINER_HOME}",
            "-e",
            "HOST=0.0.0.0",
            "-e",
            f"PORT={spec['internal_port']}",
            "-v",
            f"{workspace_mount_path}:/workspace",
            "-v",
            f"{home_mount_path}:{CONTAINER_HOME}",
            "-w",
            "/workspace",
            "-p",
            f"{spec['internal_port']}",
            APP_RUNTIME_IMAGE,
            "sh",
            "-lc",
            boot_command,
        ],
        timeout=60,
    )

    host_port = wait_for_runtime_ready(
        container_name,
        int(spec["internal_port"]),
        APP_RUNTIME_START_TIMEOUT_SECONDS,
    )
    if host_port is None:
        last_error = read_runtime_logs(container_name) or "Runtime did not become ready in time."
        remove_runtime_container(container_name)
        upsert_runtime_record(
            session_id=session["id"],
            mode="node",
            status="failed",
            entry_file=spec.get("entry_file"),
            container_name=container_name,
            host_port=None,
            internal_port=spec.get("internal_port"),
            install_command=spec.get("install_command"),
            start_command=spec.get("start_command"),
            last_error=last_error,
        )
        raise HTTPException(status_code=500, detail=last_error)

    upsert_runtime_record(
        session_id=session["id"],
        mode="node",
        status="running",
        entry_file=spec.get("entry_file"),
        container_name=container_name,
        host_port=host_port,
        internal_port=spec.get("internal_port"),
        install_command=spec.get("install_command"),
        start_command=spec.get("start_command"),
        last_error=None,
    )
    return build_runtime_payload(session)


def stop_runtime_for_session(session: dict) -> dict:
    record = fetch_runtime_record(session["id"])
    if record and record.get("container_name"):
        remove_runtime_container(record["container_name"])

    spec = detect_runtime_spec(Path(session["workspace_path"]).resolve())
    if spec["mode"] == "static":
        upsert_runtime_record(
            session_id=session["id"],
            mode="static",
            status="ready",
            entry_file=spec.get("entry_file"),
            last_error=None,
        )
    else:
        upsert_runtime_record(
            session_id=session["id"],
            mode=record["mode"] if record else spec["mode"],
            status="stopped",
            entry_file=record.get("entry_file") if record else spec.get("entry_file"),
            container_name=record.get("container_name") if record else None,
            host_port=None,
            internal_port=record.get("internal_port") if record else spec.get("internal_port"),
            install_command=record.get("install_command") if record else spec.get("install_command"),
            start_command=record.get("start_command") if record else spec.get("start_command"),
            last_error=None,
        )
    return build_runtime_payload(session)


def safe_workspace_file(workspace_path: Path, preview_path: str) -> Path:
    relative_path = preview_path.lstrip("/")
    target = (workspace_path / relative_path).resolve()
    try:
        target.relative_to(workspace_path)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Preview file not found") from exc
    return target


def build_workspace_tree_node(path: Path, workspace_path: Path) -> dict:
    relative = "" if path == workspace_path else str(path.relative_to(workspace_path)).replace("\\", "/")
    if path.is_dir():
        children = []
        for child in sorted(
            path.iterdir(),
            key=lambda item: (not item.is_dir(), item.name.lower()),
        ):
            if child.name in WORKSPACE_TREE_IGNORED_DIRS:
                continue
            children.append(build_workspace_tree_node(child, workspace_path))
        return {
            "type": "directory",
            "name": path.name if path != workspace_path else "workspace",
            "path": relative,
            "children": children,
        }

    stat = path.stat()
    return {
        "type": "file",
        "name": path.name,
        "path": relative,
        "size": stat.st_size,
        "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }


def read_workspace_file_payload(workspace_path: Path, relative_path: str) -> dict:
    target = safe_workspace_file(workspace_path, relative_path)
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    raw = target.read_bytes()
    truncated = len(raw) > WORKSPACE_FILE_MAX_BYTES
    payload = raw[:WORKSPACE_FILE_MAX_BYTES]
    is_binary = b"\x00" in payload

    return {
        "path": str(target.relative_to(workspace_path)).replace("\\", "/"),
        "name": target.name,
        "size": len(raw),
        "truncated": truncated,
        "binary": is_binary,
        "content": None if is_binary else payload.decode("utf-8", errors="replace"),
    }


def proxy_runtime_response(host_port: int, preview_path: str, request: Request) -> Response:
    upstream_path = "/" + preview_path.lstrip("/") if preview_path else "/"
    if request.url.query:
        upstream_path = f"{upstream_path}?{request.url.query}"

    conn = http.client.HTTPConnection(docker_host_gateway(), host_port, timeout=10)
    try:
        conn.request(
            request.method,
            upstream_path,
            headers={
                "Accept": request.headers.get("accept", "*/*"),
                "User-Agent": "Agent-Do-Preview-Proxy",
            },
        )
        response = conn.getresponse()
        body = response.read()
        headers = {}
        for key, value in response.getheaders():
            if key.lower() in {
                "content-type",
                "cache-control",
                "etag",
                "last-modified",
                "location",
            }:
                headers[key] = value
        headers.setdefault("Cache-Control", "no-store, max-age=0")
        return Response(content=body, status_code=response.status, headers=headers)
    except OSError as exc:
        raise HTTPException(status_code=502, detail=f"Preview proxy failed: {exc}") from exc
    finally:
        conn.close()


def build_internal_preview_url(session: dict, runtime: dict) -> str:
    if runtime.get("mode") == "node" and runtime.get("host_port"):
        return f"http://{docker_host_gateway()}:{int(runtime['host_port'])}/"
    return f"{PLAYWRIGHT_ACCEPTANCE_INTERNAL_BASE_URL}/sessions/{quote(session['id'])}/preview/"


def truncate_text(value: str, limit: int = 400) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}…"


def list_visible_children(items: list[dict], key: str = "text", limit: int = 8) -> list[str]:
    values: list[str] = []
    for item in items:
        raw = str(item.get(key) or "").strip()
        if not raw:
            continue
        values.append(truncate_text(raw, 80))
        if len(values) >= limit:
            break
    return values


def summarize_acceptance_issues(issues: list[str], warnings: list[str]) -> str:
    parts: list[str] = []
    if issues:
        parts.append("失败原因：" + "；".join(issues[:4]))
    if warnings:
        parts.append("额外警告：" + "；".join(warnings[:3]))
    return "\n".join(parts).strip()


def detect_critical_request_failures(preview_url: str, request_failures: list[str]) -> list[str]:
    parsed_preview = urlparse(preview_url)
    preview_origin = f"{parsed_preview.scheme}://{parsed_preview.netloc}" if parsed_preview.scheme and parsed_preview.netloc else ""
    critical_markers = (".css", ".js", ".mjs", ".cjs", ".ts", ".tsx", "/@vite/client", "/src/")
    critical_failures: list[str] = []
    for item in request_failures:
        text = str(item or "")
        if preview_origin and preview_origin not in text:
            continue
        lowered = text.lower()
        if any(marker in lowered for marker in critical_markers):
            critical_failures.append(truncate_text(text, 200))
    return critical_failures[:5]


def is_game_like_prompt(prompt: str) -> bool:
    lowered = str(prompt or "").lower()
    keywords = [
        "game",
        "小游戏",
        "游戏",
        "闯关",
        "runner",
        "platformer",
        "arcade",
        "puzzle",
        "match-3",
        "flappy",
        "snake",
        "2048",
        "赛车",
        "射击",
        "跳跃",
        "躲避",
    ]
    return any(keyword in lowered for keyword in keywords)


def build_dom_probe_script() -> str:
    return """
() => {
  const visible = (el) => {
    if (!(el instanceof Element)) return false;
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') === 0) {
      return false;
    }
    const rect = el.getBoundingClientRect();
    return rect.width >= 8 && rect.height >= 8;
  };

  const textNodes = Array.from(document.querySelectorAll('h1, h2, h3, p, span, button, a'))
    .filter((el) => visible(el))
    .map((el) => ({
      tag: el.tagName.toLowerCase(),
      text: (el.textContent || '').trim().replace(/\\s+/g, ' ').slice(0, 120),
    }))
    .filter((item) => item.text);

  const interactive = Array.from(document.querySelectorAll('button, [role=\"button\"], a, input[type=\"button\"], input[type=\"submit\"]'))
    .filter((el) => visible(el))
    .map((el) => ({
      tag: el.tagName.toLowerCase(),
      text: (el.textContent || el.getAttribute('aria-label') || el.getAttribute('value') || '').trim().replace(/\\s+/g, ' ').slice(0, 120),
    }));

  const mainElement = document.querySelector('main, #app, #root, body');
  const rect = mainElement ? mainElement.getBoundingClientRect() : { width: 0, height: 0 };

  return {
    title: document.title || '',
    readyState: document.readyState,
    bodyTextLength: (document.body?.innerText || '').trim().length,
    bodyTextSample: (document.body?.innerText || '').trim().replace(/\\s+/g, ' ').slice(0, 240),
    htmlLength: document.documentElement?.outerHTML?.length || 0,
    hasCanvas: Boolean(document.querySelector('canvas')),
    hasSvg: Boolean(document.querySelector('svg')),
    hasImage: Boolean(document.querySelector('img, picture, video')),
    hasMain: Boolean(mainElement),
    mainRect: { width: Math.round(rect.width || 0), height: Math.round(rect.height || 0) },
    headings: textNodes.filter((item) => /^h[1-3]$/.test(item.tag)).slice(0, 5),
    visibleText: textNodes.slice(0, 12),
    interactive,
  };
}
"""


def build_snapshot_signature(summary: dict[str, object]) -> str:
    signature_payload = {
        "title": summary.get("title"),
        "bodyTextSample": summary.get("bodyTextSample"),
        "bodyTextLength": summary.get("bodyTextLength"),
        "htmlLength": summary.get("htmlLength"),
        "hasCanvas": summary.get("hasCanvas"),
        "hasSvg": summary.get("hasSvg"),
        "hasImage": summary.get("hasImage"),
        "interactive": list_visible_children(summary.get("interactive") or []),
    }
    return sha256(json.dumps(signature_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def first_start_control(summary: dict[str, object]) -> str | None:
    candidates = summary.get("interactive") or []
    start_keywords = ("start", "play", "begin", "restart", "开始", "启动", "再来", "继续")
    for item in candidates:
        text = str(item.get("text") or "").strip().lower()
        if text and any(keyword in text for keyword in start_keywords):
            return text
    return None


def is_realtime_game_prompt(prompt: str) -> bool:
    lowered = str(prompt or "").lower()
    keywords = [
        "plane",
        "shooter",
        "shooting",
        "flight",
        "airplane",
        "飞机",
        "射击",
        "空战",
        "大战",
        "躲避",
        "runner",
        "platformer",
    ]
    return any(keyword in lowered for keyword in keywords)


def build_acceptance_report(
    *,
    preview_url: str,
    original_prompt: str,
    run_index: int,
) -> dict[str, object]:
    console_errors: list[str] = []
    page_errors: list[str] = []
    request_failures: list[str] = []
    issues: list[str] = []
    warnings: list[str] = []
    dom_summary: dict[str, object] = {}
    status_code: int | None = None
    screenshot_path = ""

    try:
        with sync_playwright() as playwright:
            browser_launcher = getattr(playwright, PLAYWRIGHT_BROWSER, None)
            if browser_launcher is None:
                raise RuntimeError(f"Unsupported Playwright browser: {PLAYWRIGHT_BROWSER}")

            browser = browser_launcher.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            try:
                context = browser.new_context(
                    viewport={
                        "width": PLAYWRIGHT_VIEWPORT_WIDTH,
                        "height": PLAYWRIGHT_VIEWPORT_HEIGHT,
                    }
                )
                page = context.new_page()
                page.on(
                    "console",
                    lambda msg: console_errors.append(f"{msg.type}: {truncate_text(msg.text, 200)}")
                    if msg.type == "error"
                    else None,
                )
                page.on(
                    "pageerror",
                    lambda exc: page_errors.append(truncate_text(str(exc), 200)),
                )
                page.on(
                    "requestfailed",
                    lambda req: request_failures.append(
                        truncate_text(f"{req.method} {req.url} -> {req.failure}", 200)
                    ),
                )

                response = page.goto(preview_url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_NAV_TIMEOUT_MS)
                page.wait_for_timeout(PLAYWRIGHT_SETTLE_MS)
                dom_summary = page.evaluate(build_dom_probe_script())
                before_signature = build_snapshot_signature(dom_summary)
                start_control = first_start_control(dom_summary)
                action_result = "none"

                if start_control:
                    try:
                        start_locator = page.locator(
                            "button, [role='button'], a, input[type='button'], input[type='submit']"
                        ).filter(has_text=start_control).first
                        start_locator.click(timeout=5000)
                        page.wait_for_timeout(PLAYWRIGHT_ACTION_SETTLE_MS)
                        if is_realtime_game_prompt(original_prompt):
                            page.keyboard.press("Space")
                            page.wait_for_timeout(300)
                            page.keyboard.press("ArrowLeft")
                            page.wait_for_timeout(300)
                        after_summary = page.evaluate(build_dom_probe_script())
                        after_signature = build_snapshot_signature(after_summary)
                        action_result = "changed" if after_signature != before_signature else "unchanged"
                        dom_summary["postAction"] = {
                            "trigger": start_control,
                            "result": action_result,
                            "bodyTextSample": after_summary.get("bodyTextSample"),
                            "interactive": list_visible_children(after_summary.get("interactive") or []),
                        }
                    except PlaywrightError as exc:
                        action_result = "failed"
                        dom_summary["postAction"] = {
                            "trigger": start_control,
                            "result": "failed",
                            "error": truncate_text(str(exc), 200),
                        }

                status_code = response.status if response else None
                if status_code and status_code >= 400:
                    issues.append(f"页面返回 HTTP {status_code}")
                if request_failures:
                    warnings.append(f"存在 {len(request_failures)} 个资源请求失败")
                    critical_request_failures = detect_critical_request_failures(preview_url, request_failures)
                    if critical_request_failures:
                        issues.append(
                            "关键静态资源加载失败："
                            + "；".join(critical_request_failures[:2])
                        )
                if page_errors:
                    issues.append(f"页面抛出了 {len(page_errors)} 个未捕获异常")
                if len(console_errors) > PLAYWRIGHT_MAX_CONSOLE_ERRORS:
                    issues.append(f"控制台出现 {len(console_errors)} 个 error 日志")

                body_text_length = int(dom_summary.get("bodyTextLength") or 0)
                has_visual_surface = bool(dom_summary.get("hasCanvas") or dom_summary.get("hasSvg") or dom_summary.get("hasImage"))
                visible_text_items = dom_summary.get("visibleText") or []
                has_visible_text = bool(visible_text_items)
                main_rect = dom_summary.get("mainRect") or {}
                if not has_visual_surface and not has_visible_text and body_text_length < 24:
                    issues.append("页面几乎没有可见内容，疑似空白页或骨架未完成")
                if int(main_rect.get("width") or 0) < 200 or int(main_rect.get("height") or 0) < 120:
                    warnings.append("主渲染区域尺寸偏小，可能没有正常铺满预览区")

                if is_game_like_prompt(original_prompt):
                    interactive_texts = list_visible_children(dom_summary.get("interactive") or [])
                    prompt_lower = str(original_prompt or "").lower()
                    body_text = str(dom_summary.get("bodyTextSample") or "")
                    body_text_lower = body_text.lower()
                    has_score = any(token in body_text_lower for token in ("score", "points")) or any(
                        token in body_text for token in ("分数", "得分", "积分")
                    )
                    has_start_affordance = bool(start_control) or any(
                        any(keyword in text.lower() for keyword in ("start", "play", "begin", "restart"))
                        or any(keyword in text for keyword in ("开始", "启动", "再来", "继续"))
                        for text in interactive_texts
                    )
                    if not has_visual_surface and not has_score:
                        issues.append("游戏页面缺少明确的玩法区域或分数状态，看起来更像普通静态介绍页")
                    if not interactive_texts:
                        issues.append("像是游戏需求，但页面缺少明显的交互控件")
                    if ("score" in prompt_lower or "points" in prompt_lower or "得分" in original_prompt or "分数" in original_prompt) and not has_score:
                        issues.append("需求提到了得分显示，但页面里没有识别到 score/分数/得分 状态")
                    if ("start" in prompt_lower or "play" in prompt_lower or "开始" in original_prompt) and not has_start_affordance:
                        issues.append("需求提到了开始游玩，但页面里没有识别到明确的开始入口")
                    if start_control and action_result == "unchanged":
                        warnings.append("检测到了开始按钮，但点击后页面状态没有明显变化")
            finally:
                browser.close()

    except Exception as exc:
        return {
            "status": "failed",
            "runIndex": run_index,
            "previewUrl": preview_url,
            "passed": False,
            "issues": [f"Playwright 验收执行失败：{truncate_text(str(exc), 240)}"],
            "warnings": [],
            "consoleErrors": [],
            "pageErrors": [],
            "requestFailures": [],
            "domSummary": {},
            "screenshotPath": "",
            "summary": f"Playwright 验收执行失败：{truncate_text(str(exc), 240)}",
        }

    status = "passed" if not issues else "failed"
    summary_bits = []
    if status_code:
        summary_bits.append(f"HTTP {status_code}")
    if dom_summary.get("title"):
        summary_bits.append(f"title={truncate_text(str(dom_summary['title']), 60)}")
    if dom_summary.get("hasCanvas"):
        summary_bits.append("canvas")
    if dom_summary.get("hasSvg"):
        summary_bits.append("svg")
    if issues:
        summary_bits.append(f"{len(issues)} issue(s)")

    return {
        "status": status,
        "runIndex": run_index,
        "previewUrl": preview_url,
        "passed": not issues,
        "issues": issues,
        "warnings": warnings,
        "consoleErrors": console_errors[:10],
        "pageErrors": page_errors[:10],
        "requestFailures": request_failures[:10],
        "domSummary": {
            **dom_summary,
            "visibleText": list_visible_children(dom_summary.get("visibleText") or []),
            "interactive": list_visible_children(dom_summary.get("interactive") or []),
        },
        "screenshotPath": screenshot_path,
        "summary": "; ".join(summary_bits) or ("验收通过" if not issues else summarize_acceptance_issues(issues, warnings)),
    }


def build_auto_repair_prompt(
    *,
    original_prompt: str,
    generation_mode: str,
    acceptance: dict[str, object],
    repair_round: int,
) -> str:
    issues = acceptance.get("issues") or []
    warnings = acceptance.get("warnings") or []
    dom_summary = acceptance.get("domSummary") or {}
    lines = [
        "Continue from the existing workspace and repair the generated project.",
        "Do not reinitialize a fresh scaffold if the current project already exists.",
        "Keep the current framework and file structure unless a small focused adjustment is needed.",
        "After editing, verify the result yourself before finishing.",
        "",
        f"Original user request:\n{original_prompt}",
        "",
        f"Generation mode: {generation_mode}",
        f"Auto-repair round: {repair_round}",
        "",
        "Browser acceptance failures:",
    ]
    if issues:
        lines.extend(f"- {item}" for item in issues[:8])
    else:
        lines.append("- 页面没有通过自动验收，请检查整体可用性。")
    if warnings:
        lines.append("")
        lines.append("Warnings:")
        lines.extend(f"- {item}" for item in warnings[:6])
    if dom_summary:
        lines.append("")
        lines.append("Observed page summary:")
        lines.append(f"- title: {truncate_text(str(dom_summary.get('title') or ''), 120) or '(empty)'}")
        lines.append(f"- bodyTextSample: {truncate_text(str(dom_summary.get('bodyTextSample') or ''), 180) or '(empty)'}")
        lines.append(f"- interactive: {', '.join(dom_summary.get('interactive') or []) or '(none)'}")
    if generation_mode == "vite":
        lines.extend(
            [
                "",
                "For this Vite project, prefer fixing the existing app instead of recreating it.",
                "Make sure package.json scripts remain runnable.",
                "If the page depends on runtime data or assets, keep paths valid under the current dev server.",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "For this single HTML project, keep everything working from the existing HTML entry file.",
            ]
        )
    return "\n".join(lines).strip()


def run_preview_acceptance_with_repair(
    session: dict,
    payload: "PreviewAcceptanceRequest",
) -> dict[str, object]:
    runtime_profile = resolve_runtime_profile(payload.runtime_profile)
    model = resolve_model(runtime_profile, payload.model)
    validate_runtime_env(runtime_profile)
    workspace_path = Path(session["workspace_path"]).resolve()

    rounds = max(0, min(payload.auto_repair_rounds, MAX_AUTO_REPAIR_ROUNDS))
    runtime = build_runtime_payload(session)
    if runtime["mode"] == "node" and runtime["status"] != "running":
        runtime = start_runtime_for_session(session)
    elif runtime["mode"] == "static" and not runtime.get("can_preview"):
        raise HTTPException(status_code=400, detail="当前 session 没有可预览内容。")

    attempts: list[dict[str, object]] = []
    repair_round = 0
    accepted = False
    last_acceptance: dict[str, object] | None = None
    repair_actions: list[dict[str, object]] = []

    while True:
        preview_url = build_internal_preview_url(session, runtime)
        acceptance = build_acceptance_report(
            preview_url=preview_url,
            original_prompt=payload.original_prompt,
            run_index=len(attempts) + 1,
        )
        last_acceptance = acceptance
        attempts.append(acceptance)
        if acceptance.get("passed"):
            accepted = True
            break
        if repair_round >= rounds:
            break
        if not workspace_has_files(workspace_path):
            repair_actions.append(
                {
                    "round": repair_round + 1,
                    "status": "skipped",
                    "reason": "workspace_empty",
                }
            )
            break

        repair_round += 1
        repair_prompt = build_auto_repair_prompt(
            original_prompt=payload.original_prompt,
            generation_mode=payload.generation_mode,
            acceptance=acceptance,
            repair_round=repair_round,
        )
        insert_message(
            session_id=session["id"],
            role="user",
            content=f"[auto-repair {repair_round}]\n{repair_prompt}",
        )
        try:
            output, exit_code, duration_ms, workspace_diff = run_claude(
                session=session,
                prompt=repair_prompt,
                model=model,
                max_turns=payload.max_turns,
                append_system_prompt=payload.append_system_prompt,
                runtime_profile=runtime_profile,
            )
        except HTTPException as exc:
            repair_actions.append(
                {
                    "round": repair_round,
                    "status": "failed",
                    "error": str(exc.detail),
                }
            )
            break

        insert_message(
            session_id=session["id"],
            role="assistant",
            content=output,
            exit_code=exit_code,
            duration_ms=duration_ms,
        )
        repair_actions.append(
            {
                "round": repair_round,
                "status": "completed",
                "changed_files": workspace_diff["changed_files"],
                "outputSummary": truncate_text(output, 240),
            }
        )

        runtime = build_runtime_payload(session)
        if runtime["mode"] == "node":
            stop_runtime_for_session(session)
            runtime = start_runtime_for_session(session)
        else:
            runtime = build_runtime_payload(session)

    final_runtime = build_runtime_payload(session)
    return {
        "status": "passed" if accepted else "failed",
        "passed": accepted,
        "attempts": attempts,
        "repairRoundsUsed": repair_round,
        "repairActions": repair_actions,
        "runtime": final_runtime,
        "acceptance": last_acceptance or {},
    }


def resolve_runtime_profile(profile: str | None) -> str:
    if profile in {"default", "aliyun"}:
        return profile
    return DEFAULT_RUNTIME_PROFILE


def validate_runtime_env(profile: str) -> dict[str, str]:
    claude_env = resolve_claude_runtime_env(profile)
    if not claude_env.get("ANTHROPIC_API_KEY") and not claude_env.get("ANTHROPIC_AUTH_TOKEN"):
        raise HTTPException(
            status_code=500,
            detail=(
                f"Claude auth is not configured for runtime profile '{profile}'. "
                "Set ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN or the corresponding ALIYUN_ANTHROPIC_* variables."
            ),
        )
    return claude_env


def resolve_claude_runtime_env(profile: str) -> dict[str, str]:
    if profile == "aliyun":
        env_map = {
            "ANTHROPIC_BASE_URL": ALIYUN_ANTHROPIC_BASE_URL,
            "ANTHROPIC_MODEL": ALIYUN_ANTHROPIC_MODEL,
        }
        if ALIYUN_ANTHROPIC_API_KEY:
            env_map["ANTHROPIC_API_KEY"] = ALIYUN_ANTHROPIC_API_KEY
        if ALIYUN_ANTHROPIC_AUTH_TOKEN:
            env_map["ANTHROPIC_AUTH_TOKEN"] = ALIYUN_ANTHROPIC_AUTH_TOKEN
        return env_map

    return {name: value for name in CLAUDE_ENV_VARS if (value := os.getenv(name))}


def resolve_model(profile: str, requested_model: str | None) -> str:
    model = (requested_model or "").strip()
    if profile == "aliyun":
        if not model or model == "sonnet":
            return ALIYUN_ANTHROPIC_MODEL
        return model

    if not model:
        return DEFAULT_CLAUDE_MODEL
    return model


def build_claude_command(
    session: dict,
    prompt: str,
    model: str,
    max_turns: int,
    append_system_prompt: str | None,
    runtime_profile: str,
) -> list[str]:
    return build_claude_exec_command(
        session=session,
        prompt=prompt,
        model=model,
        max_turns=max_turns,
        append_system_prompt=append_system_prompt,
        runtime_profile=runtime_profile,
    )


def run_claude(
    session: dict,
    prompt: str,
    model: str,
    max_turns: int,
    append_system_prompt: str | None,
    runtime_profile: str,
) -> tuple[str, int, int, dict[str, list[str]]]:
    workspace_path = Path(session["workspace_path"]).resolve()
    before_snapshot = snapshot_workspace(workspace_path)
    command = build_claude_command(
        session=session,
        prompt=prompt,
        model=model,
        max_turns=max_turns,
        append_system_prompt=append_system_prompt,
        runtime_profile=runtime_profile,
    )

    started = perf_counter()
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=CLAUDE_TIMEOUT_SECONDS,
            check=False,
        )
    except FileNotFoundError as exc:
        runtime = fetch_claude_runtime_record(session["id"])
        if runtime:
            upsert_claude_runtime_record(
                session_id=session["id"],
                container_name=runtime["container_name"],
                status="running",
                last_error=f"Missing runtime dependency: {exc}",
                last_used_at=utc_now(),
            )
        raise HTTPException(status_code=500, detail=f"Missing runtime dependency: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        runtime = fetch_claude_runtime_record(session["id"])
        if runtime:
            upsert_claude_runtime_record(
                session_id=session["id"],
                container_name=runtime["container_name"],
                status="running",
                last_error="Claude execution timed out",
                last_used_at=utc_now(),
            )
        raise HTTPException(status_code=504, detail="Claude execution timed out") from exc

    duration_ms = int((perf_counter() - started) * 1000)
    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    parsed_output, parsed_error = extract_claude_result(stdout)

    if completed.returncode != 0:
        runtime = fetch_claude_runtime_record(session["id"])
        if runtime:
            upsert_claude_runtime_record(
                session_id=session["id"],
                container_name=runtime["container_name"],
                status="running",
                last_error=parsed_error or stderr or parsed_output or stdout or f"Claude failed with exit code {completed.returncode}",
                last_used_at=utc_now(),
            )
        detail = parsed_error or stderr or parsed_output or stdout or f"Claude failed with exit code {completed.returncode}"
        raise HTTPException(status_code=500, detail=detail)
    if parsed_error:
        runtime = fetch_claude_runtime_record(session["id"])
        if runtime:
            upsert_claude_runtime_record(
                session_id=session["id"],
                container_name=runtime["container_name"],
                status="running",
                last_error=parsed_error,
                last_used_at=utc_now(),
            )
        raise HTTPException(status_code=500, detail=parsed_error)

    after_snapshot = snapshot_workspace(workspace_path)
    workspace_diff = diff_workspace(before_snapshot, after_snapshot)
    if maybe_materialize_single_html_output(prompt, workspace_path, parsed_output or stdout, workspace_diff):
        after_snapshot = snapshot_workspace(workspace_path)
        workspace_diff = diff_workspace(before_snapshot, after_snapshot)
    annotated_output = maybe_annotate_output(prompt, parsed_output or stdout, workspace_diff)
    finished_at = utc_now()
    runtime = fetch_claude_runtime_record(session["id"])
    if runtime:
        upsert_claude_runtime_record(
            session_id=session["id"],
            container_name=runtime["container_name"],
            status="running",
            last_error=None,
            last_used_at=finished_at,
        )

    return annotated_output, completed.returncode, duration_ms, workspace_diff


def cleanup_idle_claude_runtimes(stop_event: threading.Event) -> None:
    idle_label = format_duration(CLAUDE_IDLE_TIMEOUT_SECONDS)
    while True:
        cutoff = time.time() - CLAUDE_IDLE_TIMEOUT_SECONDS
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT session_id, container_name, status, last_error, created_at, updated_at, last_used_at
                FROM claude_runtimes
                WHERE status IN ('running', 'busy')
                """
            ).fetchall()
        for row in rows:
            record = refresh_claude_runtime_record(row_to_dict(row))
            if not record or record.get("status") not in {"running", "busy"}:
                continue
            last_used_at = record.get("last_used_at")
            if not last_used_at:
                continue
            try:
                last_used_ts = datetime.fromisoformat(last_used_at).timestamp()
            except ValueError:
                continue
            if last_used_ts > cutoff:
                continue
            remove_runtime_container(record["container_name"])
            upsert_claude_runtime_record(
                session_id=record["session_id"],
                container_name=record["container_name"],
                status="stopped",
                last_error=f"Claude container stopped after {idle_label} of inactivity.",
                last_used_at=record["last_used_at"],
            )
            LOGGER.info(
                "Stopped idle claude container %s for session %s",
                record["container_name"],
                record["session_id"],
            )
        if stop_event.wait(CLAUDE_IDLE_SWEEP_SECONDS):
            break


def cleanup_idle_preview_runtimes(stop_event: threading.Event) -> None:
    idle_label = format_duration(APP_RUNTIME_IDLE_TIMEOUT_SECONDS)
    while True:
        cutoff = time.time() - APP_RUNTIME_IDLE_TIMEOUT_SECONDS
        with get_conn() as conn:
            runtime_rows = conn.execute(
                """
                SELECT *
                FROM runtimes
                WHERE mode = 'node' AND status IN ('running', 'starting')
                """
            ).fetchall()
            session_rows = conn.execute(
                "SELECT id, last_active_at FROM sessions"
            ).fetchall()

        session_activity = {
            row["id"]: parse_iso_timestamp(row["last_active_at"])
            for row in session_rows
        }

        for row in runtime_rows:
            record = refresh_runtime_record(row_to_dict(row))
            if not record or record.get("status") != "running" or not record.get("container_name"):
                continue

            last_activity = max(
                [
                    ts
                    for ts in (
                        parse_iso_timestamp(record.get("updated_at")),
                        session_activity.get(record["session_id"]),
                    )
                    if ts is not None
                ],
                default=None,
            )
            if last_activity is None or last_activity > cutoff:
                continue

            remove_runtime_container(record["container_name"])
            upsert_runtime_record(
                session_id=record["session_id"],
                mode=record["mode"],
                status="stopped",
                entry_file=record.get("entry_file"),
                container_name=record.get("container_name"),
                host_port=None,
                internal_port=record.get("internal_port"),
                install_command=record.get("install_command"),
                start_command=record.get("start_command"),
                last_error=f"Preview runtime stopped after {idle_label} of inactivity.",
            )
            LOGGER.info(
                "Stopped idle preview runtime %s for session %s",
                record["container_name"],
                record["session_id"],
            )

        if stop_event.wait(APP_RUNTIME_IDLE_SWEEP_SECONDS):
            break


def cleanup_orphaned_agentdo_containers(stop_event: threading.Event) -> None:
    while True:
        with get_conn() as conn:
            active_runtime_rows = conn.execute(
                """
                SELECT container_name
                FROM runtimes
                WHERE container_name IS NOT NULL AND status IN ('running', 'starting')
                """
            ).fetchall()
            active_claude_rows = conn.execute(
                """
                SELECT container_name
                FROM claude_runtimes
                WHERE container_name IS NOT NULL AND status IN ('running', 'busy', 'starting')
                """
            ).fetchall()

        active_container_names = {
            row["container_name"]
            for row in [*active_runtime_rows, *active_claude_rows]
            if row["container_name"]
        }

        for container_name in list_agentdo_container_names():
            if container_name in active_container_names:
                continue
            remove_runtime_container(container_name)
            LOGGER.info("Removed orphaned Agent-Do container %s", container_name)

        if stop_event.wait(AGENTDO_ORPHAN_SWEEP_SECONDS):
            break


def cleanup_expired_sessions(stop_event: threading.Event) -> None:
    retention_label = format_duration(AGENT_SESSION_RETENTION_SECONDS)
    while True:
        cutoff = time.time() - AGENT_SESSION_RETENTION_SECONDS
        with get_conn() as conn:
            session_rows = conn.execute(
                "SELECT id, last_active_at FROM sessions"
            ).fetchall()
            runtime_rows = conn.execute(
                "SELECT session_id, updated_at FROM runtimes"
            ).fetchall()
            claude_rows = conn.execute(
                "SELECT session_id, last_used_at FROM claude_runtimes"
            ).fetchall()

        runtime_activity = {
            row["session_id"]: parse_iso_timestamp(row["updated_at"])
            for row in runtime_rows
        }
        claude_activity = {
            row["session_id"]: parse_iso_timestamp(row["last_used_at"])
            for row in claude_rows
        }

        for row in session_rows:
            last_activity = max(
                [
                    ts
                    for ts in (
                        parse_iso_timestamp(row["last_active_at"]),
                        runtime_activity.get(row["id"]),
                        claude_activity.get(row["id"]),
                    )
                    if ts is not None
                ],
                default=None,
            )
            if last_activity is None or last_activity > cutoff:
                continue
            try:
                delete_session_data(row["id"])
                LOGGER.info(
                    "Deleted expired Agent-Do session %s after %s of inactivity",
                    row["id"],
                    retention_label,
                )
            except HTTPException as exc:
                if exc.status_code != 404:
                    LOGGER.warning("Failed to delete expired session %s: %s", row["id"], exc.detail)
            except Exception:
                LOGGER.exception("Failed to delete expired session %s", row["id"])

        if stop_event.wait(AGENT_SESSION_RETENTION_SWEEP_SECONDS):
            break


def sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def read_available(stream) -> bytes:
    try:
        if hasattr(stream, "read1"):
            chunk = stream.read1(4096)
        else:
            chunk = stream.read(4096)
    except BlockingIOError:
        return b""
    return chunk or b""


def iter_claude_json_lines(buffer: str) -> tuple[list[dict], str]:
    items: list[dict] = []
    lines = buffer.splitlines(keepends=True)
    remainder = ""
    if lines and not lines[-1].endswith("\n"):
        remainder = lines.pop()
    for line in lines:
        payload = line.strip()
        if not payload:
            continue
        try:
            item = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            items.append(item)
    if remainder:
        try:
            item = json.loads(remainder.strip())
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(item, dict):
                items.append(item)
                remainder = ""
    return items, remainder


def extract_claude_stream_updates(item: dict) -> list[tuple[str, dict]]:
    item_type = str(item.get("type") or "")
    updates: list[tuple[str, dict]] = []

    if item_type == "stream_event":
        event = item.get("event") or {}
        event_type = str(event.get("type") or "")
        if event_type == "content_block_delta":
            delta = event.get("delta") or {}
            if delta.get("type") == "text_delta" and delta.get("text"):
                updates.append(("chunk", {"text": str(delta["text"])}))
        elif event_type == "content_block_start":
            block = event.get("content_block") or {}
            if block.get("type") == "tool_use":
                tool_input = block.get("input")
                updates.append((
                    "tool",
                    {
                        "tool": str(block.get("name") or "tool"),
                        "title": str(block.get("name") or "tool"),
                        "status": "running",
                        "input": json.dumps(tool_input, ensure_ascii=False, indent=2) if tool_input else "",
                    },
                ))
        return updates

    if item_type == "assistant":
        for block in item.get("message", {}).get("content", []) or []:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                tool_input = block.get("input")
                updates.append((
                    "tool",
                    {
                        "tool": str(block.get("name") or "tool"),
                        "title": str(block.get("name") or "tool"),
                        "status": "running",
                        "input": json.dumps(tool_input, ensure_ascii=False, indent=2) if tool_input else "",
                    },
                ))
        return updates

    if item_type == "user":
        tool_result = item.get("tool_use_result")
        if isinstance(tool_result, dict):
            title = str(tool_result.get("type") or "tool")
            file_path = str(tool_result.get("filePath") or "")
            output = file_path or json.dumps(tool_result, ensure_ascii=False, indent=2)
            updates.append((
                "tool",
                {
                    "tool": title,
                    "title": f"{title} result",
                    "status": "completed",
                    "output": output,
                },
            ))
        return updates

    if item_type == "result" and item.get("is_error"):
        updates.append((
            "error",
            {
                "message": str(item.get("result") or "Claude stream failed"),
            },
        ))

    return updates


def extract_claude_result(stdout: str) -> tuple[str, str | None]:
    text_parts: list[str] = []
    result_text = ""
    error_text: str | None = None

    items, _ = iter_claude_json_lines(stdout)
    for item in items:
        if item.get("type") == "stream_event":
            event = item.get("event") or {}
            if event.get("type") == "content_block_delta":
                delta = event.get("delta") or {}
                if delta.get("type") == "text_delta" and delta.get("text"):
                    text_parts.append(str(delta["text"]))
        elif item.get("type") == "result":
            if item.get("result"):
                result_text = str(item["result"])
            if item.get("is_error"):
                error_text = str(item.get("result") or "Claude command failed")

    final_text = result_text.strip() or "".join(text_parts).strip()
    return final_text, error_text


def stream_claude(
    session: dict,
    prompt: str,
    model: str,
    max_turns: int,
    append_system_prompt: str | None,
    runtime_profile: str,
):
    workspace_path = Path(session["workspace_path"]).resolve()
    before_snapshot = snapshot_workspace(workspace_path)
    command = build_claude_command(
        session=session,
        prompt=prompt,
        model=model,
        max_turns=max_turns,
        append_system_prompt=append_system_prompt,
        runtime_profile=runtime_profile,
    )

    started = perf_counter()
    yield sse_event("started", {"session_id": session["id"], "timestamp": utc_now()})

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
        )
    except FileNotFoundError as exc:
        runtime = fetch_claude_runtime_record(session["id"])
        if runtime:
            upsert_claude_runtime_record(
                session_id=session["id"],
                container_name=runtime["container_name"],
                status="running",
                last_error=f"Missing runtime dependency: {exc}",
                last_used_at=utc_now(),
            )
        yield sse_event("error", {"message": f"Missing runtime dependency: {exc}"})
        return

    assert process.stdout is not None
    os.set_blocking(process.stdout.fileno(), False)

    raw_output_chunks: list[str] = []
    visible_text_chunks: list[str] = []
    json_buffer = ""
    claude_error: str | None = None
    deadline = perf_counter() + CLAUDE_TIMEOUT_SECONDS
    last_keepalive_at = perf_counter()

    while True:
        if perf_counter() > deadline:
            process.kill()
            process.wait()
            runtime = fetch_claude_runtime_record(session["id"])
            if runtime:
                upsert_claude_runtime_record(
                    session_id=session["id"],
                    container_name=runtime["container_name"],
                    status="running",
                    last_error="Claude execution timed out",
                    last_used_at=utc_now(),
                )
            yield sse_event("error", {"message": "Claude execution timed out"})
            return

        chunk = read_available(process.stdout)
        if chunk:
            text = chunk.decode("utf-8", errors="replace")
            raw_output_chunks.append(text)
            json_buffer += text
            last_keepalive_at = perf_counter()
            items, json_buffer = iter_claude_json_lines(json_buffer)
            for item in items:
                for event_name, payload in extract_claude_stream_updates(item):
                    if event_name == "chunk" and payload.get("text"):
                        visible_text_chunks.append(str(payload["text"]))
                    if event_name == "error":
                        claude_error = str(payload.get("message") or "Claude stream failed")
                    yield sse_event(event_name, payload)
            continue

        if process.poll() is not None:
            tail = read_available(process.stdout)
            if tail:
                text = tail.decode("utf-8", errors="replace")
                raw_output_chunks.append(text)
                json_buffer += text
            break

        now = perf_counter()
        if now - last_keepalive_at >= STREAM_KEEPALIVE_INTERVAL_SECONDS:
            yield sse_event("ping", {"timestamp": utc_now()})
            last_keepalive_at = now

        time.sleep(0.05)

    duration_ms = int((perf_counter() - started) * 1000)
    items, json_buffer = iter_claude_json_lines(json_buffer)
    for item in items:
        for event_name, payload in extract_claude_stream_updates(item):
            if event_name == "chunk" and payload.get("text"):
                visible_text_chunks.append(str(payload["text"]))
            if event_name == "error":
                claude_error = str(payload.get("message") or "Claude stream failed")
            yield sse_event(event_name, payload)

    raw_output = "".join(raw_output_chunks).strip()
    parsed_output, parsed_error = extract_claude_result(raw_output)
    output = parsed_output or "".join(visible_text_chunks).strip()
    fallback_output = output or raw_output
    exit_code = process.returncode or 0

    if claude_error or parsed_error or exit_code != 0:
        runtime = fetch_claude_runtime_record(session["id"])
        if runtime:
            upsert_claude_runtime_record(
                session_id=session["id"],
                container_name=runtime["container_name"],
                status="running",
                last_error=claude_error or parsed_error or fallback_output or f"Claude failed with exit code {exit_code}",
                last_used_at=utc_now(),
            )
        detail = claude_error or parsed_error or fallback_output or f"Claude failed with exit code {exit_code}"
        yield sse_event(
            "error",
            {
                "message": detail,
                "exit_code": exit_code,
                "duration_ms": duration_ms,
            },
        )
        return

    after_snapshot = snapshot_workspace(workspace_path)
    workspace_diff = diff_workspace(before_snapshot, after_snapshot)
    if maybe_materialize_single_html_output(prompt, workspace_path, fallback_output, workspace_diff):
        after_snapshot = snapshot_workspace(workspace_path)
        workspace_diff = diff_workspace(before_snapshot, after_snapshot)
    annotated_output = maybe_annotate_output(prompt, fallback_output, workspace_diff)
    finished_at = utc_now()
    runtime = fetch_claude_runtime_record(session["id"])
    if runtime:
        upsert_claude_runtime_record(
            session_id=session["id"],
            container_name=runtime["container_name"],
            status="running",
            last_error=None,
            last_used_at=finished_at,
        )

    insert_message(
        session_id=session["id"],
        role="assistant",
        content=annotated_output,
        exit_code=exit_code,
        duration_ms=duration_ms,
        created_at=finished_at,
    )

    yield sse_event(
        "done",
        {
            "output": annotated_output,
            "exit_code": exit_code,
            "duration_ms": duration_ms,
            "changed_files": workspace_diff["changed_files"],
            "added_files": workspace_diff["added"],
            "modified_files": workspace_diff["modified"],
            "deleted_files": workspace_diff["deleted"],
        },
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_data_dirs()
    init_db()
    stop_event = threading.Event()
    workers: list[threading.Thread] = []
    if CLAUDE_IDLE_TIMEOUT_SECONDS > 0 and CLAUDE_IDLE_SWEEP_SECONDS > 0:
        workers.append(
            threading.Thread(
                target=cleanup_idle_claude_runtimes,
                args=(stop_event,),
                name="claude-runtime-cleanup",
                daemon=True,
            )
        )
    if APP_RUNTIME_IDLE_TIMEOUT_SECONDS > 0 and APP_RUNTIME_IDLE_SWEEP_SECONDS > 0:
        workers.append(
            threading.Thread(
                target=cleanup_idle_preview_runtimes,
                args=(stop_event,),
                name="preview-runtime-cleanup",
                daemon=True,
            )
        )
    if AGENTDO_ORPHAN_SWEEP_SECONDS > 0:
        workers.append(
            threading.Thread(
                target=cleanup_orphaned_agentdo_containers,
                args=(stop_event,),
                name="agentdo-orphan-cleanup",
                daemon=True,
            )
        )
    if AGENT_SESSION_RETENTION_SECONDS > 0 and AGENT_SESSION_RETENTION_SWEEP_SECONDS > 0:
        workers.append(
            threading.Thread(
                target=cleanup_expired_sessions,
                args=(stop_event,),
                name="agentdo-session-retention",
                daemon=True,
            )
        )

    for worker in workers:
        worker.start()
    try:
        yield
    finally:
        stop_event.set()
        for worker in workers:
            worker.join(timeout=2)


app = FastAPI(title="Agent-Do MVP", lifespan=lifespan)


class CreateSessionRequest(BaseModel):
    user_id: str = Field(default=DEFAULT_USER_ID)
    title: str | None = None


class SendMessageRequest(BaseModel):
    content: str = Field(min_length=1)
    model: str | None = None
    max_turns: int = Field(default=8, ge=1, le=20)
    append_system_prompt: str | None = None
    runtime_profile: str | None = None


class RuntimeActionRequest(BaseModel):
    restart: bool = Field(default=False)


class PreviewAcceptanceRequest(BaseModel):
    original_prompt: str = Field(default="")
    generation_mode: str = Field(default="single_html")
    model: str | None = None
    max_turns: int = Field(default=8, ge=1, le=20)
    append_system_prompt: str | None = None
    runtime_profile: str | None = None
    auto_repair_rounds: int = Field(default=DEFAULT_AUTO_REPAIR_ROUNDS, ge=0, le=3)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(
        STATIC_ROOT / "index.html",
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@app.get("/healthz")
def healthcheck() -> dict:
    return {"status": "ok", "timestamp": utc_now()}


@app.post("/sessions")
def create_session(payload: CreateSessionRequest) -> dict:
    session_id = str(uuid.uuid4())
    session_root = SESSIONS_ROOT / session_id
    workspace_path = session_root / "workspace"
    home_path = session_root / "home"

    workspace_path.mkdir(parents=True, exist_ok=True)
    home_path.mkdir(parents=True, exist_ok=True)
    os.chmod(workspace_path, 0o777)
    os.chmod(home_path, 0o777)

    now = utc_now()
    session = {
        "id": session_id,
        "user_id": payload.user_id,
        "title": payload.title,
        "workspace_path": str(workspace_path),
        "home_path": str(home_path),
        "created_at": now,
        "last_active_at": now,
    }

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO sessions (id, user_id, title, workspace_path, home_path, created_at, last_active_at)
            VALUES (:id, :user_id, :title, :workspace_path, :home_path, :created_at, :last_active_at)
            """,
            session,
        )
        conn.commit()

    return build_session_payload(session_id)


@app.get("/sessions")
def list_sessions() -> dict:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM sessions ORDER BY last_active_at DESC"
        ).fetchall()
    return {"items": [build_session_payload(row["id"]) for row in rows]}


@app.get("/sessions/{session_id}")
def get_session(session_id: str) -> dict:
    return build_session_payload(session_id)


@app.delete("/sessions/{session_id}")
def delete_session(session_id: str) -> dict:
    session = delete_session_data(session_id)
    return {"ok": True, "deleted_session_id": session_id, "title": session.get("title")}


@app.post("/sessions/{session_id}/claude-runtime/stop")
def stop_claude_runtime(session_id: str) -> dict:
    session = fetch_session(session_id)
    return stop_claude_runtime_for_session(
        session,
        reason="Claude runtime stopped after request completion.",
    )


@app.get("/sessions/{session_id}/messages")
def list_messages(session_id: str) -> dict:
    fetch_session(session_id)
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, session_id, role, content, exit_code, duration_ms, created_at
            FROM messages
            WHERE session_id = ?
            ORDER BY created_at ASC
            """,
            (session_id,),
        ).fetchall()
    return {"items": [row_to_dict(row) for row in rows]}


@app.get("/sessions/{session_id}/runtime")
def get_runtime(session_id: str) -> dict:
    session = fetch_session(session_id)
    return build_runtime_payload(session)


@app.post("/sessions/{session_id}/runtime/start")
def start_runtime(session_id: str, payload: RuntimeActionRequest | None = None) -> dict:
    session = fetch_session(session_id)
    existing = refresh_runtime_record(fetch_runtime_record(session["id"]))
    if existing and existing["mode"] == "node" and existing["status"] == "running" and not (payload and payload.restart):
        return build_runtime_payload(session)
    if payload and payload.restart:
        stop_runtime_for_session(session)
    return start_runtime_for_session(session)


@app.post("/sessions/{session_id}/runtime/stop")
def stop_runtime(session_id: str) -> dict:
    session = fetch_session(session_id)
    return stop_runtime_for_session(session)


@app.get("/sessions/{session_id}/runtime/logs")
def get_runtime_logs(session_id: str) -> dict:
    session = fetch_session(session_id)
    record = refresh_runtime_record(fetch_runtime_record(session["id"]))
    if not record or not record.get("container_name"):
        return {"session_id": session_id, "logs": "", "status": "not_running"}
    return {
        "session_id": session_id,
        "logs": read_runtime_logs(record["container_name"]),
        "status": record["status"],
    }


@app.post("/sessions/{session_id}/acceptance")
def run_session_acceptance(session_id: str, payload: PreviewAcceptanceRequest) -> dict:
    session = fetch_session(session_id)
    return run_preview_acceptance_with_repair(session, payload)


@app.get("/sessions/{session_id}/workspace/tree")
def get_workspace_tree(session_id: str) -> dict:
    session = fetch_session(session_id)
    workspace_path = Path(session["workspace_path"]).resolve()
    return {
        "session_id": session_id,
        "root": build_workspace_tree_node(workspace_path, workspace_path),
        "ignored_dirs": sorted(WORKSPACE_TREE_IGNORED_DIRS),
    }


@app.get("/sessions/{session_id}/workspace/file")
def get_workspace_file(session_id: str, path: str) -> dict:
    session = fetch_session(session_id)
    workspace_path = Path(session["workspace_path"]).resolve()
    payload = read_workspace_file_payload(workspace_path, path)
    payload["session_id"] = session_id
    return payload


@app.get("/sessions/{session_id}/preview")
@app.get("/sessions/{session_id}/preview/{preview_path:path}")
def preview_session(session_id: str, request: Request, preview_path: str = ""):
    session = fetch_session(session_id)
    runtime = build_runtime_payload(session)

    if runtime["mode"] == "node":
        if runtime["status"] != "running" or not runtime.get("host_port"):
            raise HTTPException(status_code=409, detail="项目尚未运行，请先启动预览。")
        touch_runtime_record(fetch_runtime_record(session_id))
        return proxy_runtime_response(int(runtime["host_port"]), preview_path, request)

    if runtime["mode"] == "static":
        workspace_path = Path(session["workspace_path"]).resolve()
        entry_file = runtime.get("entry_file")
        if not entry_file:
            raise HTTPException(status_code=404, detail="未找到可预览的 HTML 文件。")
        if not preview_path:
            quoted_entry = quote(entry_file)
            return RedirectResponse(url=f"/sessions/{session_id}/preview/{quoted_entry}")
        target = safe_workspace_file(workspace_path, preview_path)
        if not target.exists() or not target.is_file():
            raise HTTPException(status_code=404, detail="Preview file not found")
        media_type = "text/html; charset=utf-8" if target.suffix.lower() == ".html" else None
        return FileResponse(
            target,
            media_type=media_type,
            headers={"Cache-Control": "no-store, max-age=0"},
        )

    raise HTTPException(status_code=404, detail="当前 session 没有可预览内容。")


@app.post("/sessions/{session_id}/messages")
def send_message(session_id: str, payload: SendMessageRequest) -> dict:
    session = fetch_session(session_id)
    ensure_claude_runtime_for_session(session)
    insert_message(session_id=session_id, role="user", content=payload.content)
    runtime_profile = resolve_runtime_profile(payload.runtime_profile)
    model = resolve_model(runtime_profile, payload.model)
    validate_runtime_env(runtime_profile)

    output, exit_code, duration_ms, workspace_diff = run_claude(
        session=session,
        prompt=payload.content,
        model=model,
        max_turns=payload.max_turns,
        append_system_prompt=payload.append_system_prompt,
        runtime_profile=runtime_profile,
    )

    finished_at = utc_now()
    runtime = fetch_claude_runtime_record(session_id)
    if runtime:
        upsert_claude_runtime_record(
            session_id=session_id,
            container_name=runtime["container_name"],
            status="running",
            last_error=None,
            last_used_at=finished_at,
        )
    insert_message(
        session_id=session_id,
        role="assistant",
        content=output,
        exit_code=exit_code,
        duration_ms=duration_ms,
        created_at=finished_at,
    )

    return {
        "session_id": session_id,
        "output": output,
        "exit_code": exit_code,
        "duration_ms": duration_ms,
        "runtime_profile": runtime_profile,
        "model": model,
        "changed_files": workspace_diff["changed_files"],
        "added_files": workspace_diff["added"],
        "modified_files": workspace_diff["modified"],
        "deleted_files": workspace_diff["deleted"],
        "claude_runtime": build_session_payload(session_id)["claude_runtime"],
    }


@app.post("/sessions/{session_id}/messages/stream")
def send_message_stream(session_id: str, payload: SendMessageRequest) -> StreamingResponse:
    session = fetch_session(session_id)
    ensure_claude_runtime_for_session(session)
    insert_message(session_id=session_id, role="user", content=payload.content)
    runtime_profile = resolve_runtime_profile(payload.runtime_profile)
    model = resolve_model(runtime_profile, payload.model)
    validate_runtime_env(runtime_profile)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }

    return StreamingResponse(
        stream_claude(
            session=session,
            prompt=payload.content,
            model=model,
            max_turns=payload.max_turns,
            append_system_prompt=payload.append_system_prompt,
            runtime_profile=runtime_profile,
        ),
        media_type="text/event-stream",
        headers=headers,
    )


@app.get("/runtime-profiles")
def list_runtime_profiles() -> dict:
    return {
        "default_runtime_profile": DEFAULT_RUNTIME_PROFILE,
        "items": [
            {
                "id": "default",
                "label": "Claude Default",
                "default_model": DEFAULT_CLAUDE_MODEL,
                "configured": bool(resolve_claude_runtime_env("default").get("ANTHROPIC_API_KEY") or resolve_claude_runtime_env("default").get("ANTHROPIC_AUTH_TOKEN")),
            },
            {
                "id": "aliyun",
                "label": "Claude via Aliyun",
                "default_model": ALIYUN_ANTHROPIC_MODEL,
                "configured": bool(ALIYUN_ANTHROPIC_API_KEY or ALIYUN_ANTHROPIC_AUTH_TOKEN),
                "base_url": ALIYUN_ANTHROPIC_BASE_URL,
            },
        ],
    }
