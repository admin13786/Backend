import asyncio
import io
import json
import logging
import os
import re
import sqlite3
import time
import uuid
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

import oss2
from dotenv import load_dotenv
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from openai import AsyncOpenAI
from pydantic import BaseModel

from metrics import Trace, record_stream_event
from skill_store import skill_router

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency
    PdfReader = None


load_dotenv(Path(__file__).resolve().with_name(".env"), override=False)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("workshop.agentdo")

ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID")
ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET")
BUCKET_NAME = os.getenv("OSS_BUCKET_NAME")
ENDPOINT = os.getenv("OSS_ENDPOINT")
DOMAIN = os.getenv("OSS_DOMAIN")

DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
DASHSCOPE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"

AGENT_DO_BASE_URL = os.getenv("AGENT_DO_BASE_URL", "").rstrip("/")
AGENT_DO_DEFAULT_MODEL = os.getenv("AGENT_DO_DEFAULT_MODEL", "sonnet")
AGENT_DO_MAX_TURNS = int(os.getenv("AGENT_DO_MAX_TURNS", "8"))
AGENT_DO_AUTO_REPAIR_ROUNDS = int(os.getenv("AGENT_DO_AUTO_REPAIR_ROUNDS", "1"))
AGENT_DO_ACCEPTANCE_TIMEOUT = int(os.getenv("AGENT_DO_ACCEPTANCE_TIMEOUT", "240"))
AGENT_DO_RELEASE_CLAUDE_AFTER_REQUEST = os.getenv("AGENT_DO_RELEASE_CLAUDE_AFTER_REQUEST", "true").lower() not in {
    "0",
    "false",
    "no",
}
WORKSHOP_PUBLIC_API_BASE = os.getenv("WORKSHOP_PUBLIC_API_BASE", "/api/workshop").rstrip("/")
SESSION_MAP_DB = Path(
    os.getenv(
        "WORKSHOP_AGENTDO_MAP_DB",
        str(Path(__file__).resolve().parent / "agentdo_session_map.db"),
    )
)

app = FastAPI()
app.include_router(skill_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

HTML_BEGIN_MARKER = "<<<HTML_BEGIN>>>"
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}
TEXT_ATTACHMENT_EXTENSIONS = {
    ".txt",
    ".md",
    ".markdown",
    ".csv",
    ".json",
    ".yaml",
    ".yml",
    ".xml",
    ".html",
    ".htm",
    ".py",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".java",
    ".c",
    ".cc",
    ".cpp",
    ".h",
    ".hpp",
    ".css",
    ".sql",
    ".sh",
}
ATTACHMENT_EXTRACTION_MAX_CHARS = 120000


class _StreamPhaseSplitter:
    def __init__(self, marker: str = HTML_BEGIN_MARKER):
        self.marker = marker
        self.phase_html = False
        self._hold = ""

    def feed(self, piece: str) -> list[tuple[str, str]]:
        s = self._hold + piece
        self._hold = ""
        out: list[tuple[str, str]] = []
        if not self.phase_html:
            idx = s.find(self.marker)
            if idx != -1:
                pre = s[:idx]
                s = s[idx + len(self.marker) :]
                self.phase_html = True
                if pre:
                    out.append(("friendly", pre))
            else:
                keep = len(self.marker) - 1
                if len(s) <= keep:
                    self._hold = s
                    return out
                for k in range(keep, 0, -1):
                    if s.endswith(self.marker[:k]):
                        safe = s[:-k]
                        self._hold = s[-k:]
                        if safe:
                            out.append(("friendly", safe))
                        return out
                out.append(("friendly", s))
                return out
        if self.phase_html and s:
            out.append(("html", s))
        return out

    def flush(self) -> list[tuple[str, str]]:
        if not self._hold:
            return []
        kind = "html" if self.phase_html else "friendly"
        tail = self._hold
        self._hold = ""
        return [(kind, tail)] if tail else []


class GenerateRequest(BaseModel):
    context: str
    system_prompt: str


class AgentDoGenerateRequest(BaseModel):
    context: str
    system_prompt: str = ""
    conversation_id: str
    username: str = "workshop_guest"
    title: str = ""
    generation_mode: str = "single_html"


class AgentDoSessionEnsureRequest(BaseModel):
    conversation_id: str
    username: str = "workshop_guest"
    title: str = ""


class AgentDoSessionRestoreRequest(BaseModel):
    conversation_id: str
    agentdo_session_id: str
    username: str = "workshop_guest"
    workspace_path: str = ""


def _session_map_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(SESSION_MAP_DB)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS conversation_sessions (
            username TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            agentdo_session_id TEXT NOT NULL,
            workspace_path TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (username, conversation_id)
        )
        """
    )
    return conn


def _get_session_mapping(username: str, conversation_id: str) -> dict[str, Any] | None:
    with _session_map_conn() as conn:
        row = conn.execute(
            """
            SELECT username, conversation_id, agentdo_session_id, workspace_path, created_at, updated_at
            FROM conversation_sessions
            WHERE username = ? AND conversation_id = ?
            """,
            (username, conversation_id),
        ).fetchone()
    return dict(row) if row else None


def _save_session_mapping(
    username: str,
    conversation_id: str,
    agentdo_session_id: str,
    workspace_path: str = "",
) -> None:
    with _session_map_conn() as conn:
        conn.execute(
            """
            INSERT INTO conversation_sessions (username, conversation_id, agentdo_session_id, workspace_path, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(username, conversation_id) DO UPDATE SET
                agentdo_session_id = excluded.agentdo_session_id,
                workspace_path = excluded.workspace_path,
                updated_at = datetime('now')
            """,
            (username, conversation_id, agentdo_session_id, workspace_path),
        )
        conn.commit()


def _delete_session_mapping(username: str, conversation_id: str) -> dict[str, Any] | None:
    with _session_map_conn() as conn:
        row = conn.execute(
            """
            SELECT username, conversation_id, agentdo_session_id, workspace_path, created_at, updated_at
            FROM conversation_sessions
            WHERE username = ? AND conversation_id = ?
            """,
            (username, conversation_id),
        ).fetchone()
        if not row:
            return None
        conn.execute(
            """
            DELETE FROM conversation_sessions
            WHERE username = ? AND conversation_id = ?
            """,
            (username, conversation_id),
        )
        conn.commit()
    return dict(row)


def _build_session_mapping_response(
    username: str,
    conversation_id: str,
    agentdo_session_id: str,
    workspace_path: str = "",
) -> dict[str, Any]:
    normalized_workspace_path = str(workspace_path or "")
    return {
        "username": username,
        "conversationId": conversation_id,
        "conversation_id": conversation_id,
        "agentDoSessionId": agentdo_session_id,
        "agentdo_session_id": agentdo_session_id,
        "workspacePath": normalized_workspace_path,
        "workspace_path": normalized_workspace_path,
    }


def _list_session_mappings_for_user(username: str) -> list[dict[str, Any]]:
    with _session_map_conn() as conn:
        rows = conn.execute(
            """
            SELECT username, conversation_id, agentdo_session_id, workspace_path, created_at, updated_at
            FROM conversation_sessions
            WHERE username = ?
            ORDER BY updated_at DESC, created_at DESC
            """,
            (username,),
        ).fetchall()
    return [dict(row) for row in rows]


def _empty_conversation_token_usage(
    username: str,
    conversation_id: str,
    agentdo_session_id: str = "",
    workspace_path: str = "",
    title: str = "",
) -> dict[str, Any]:
    normalized_title = str(title or conversation_id or "")
    return {
        **_build_session_mapping_response(username, conversation_id, agentdo_session_id, workspace_path),
        "tokenUsage": {
            "session_id": agentdo_session_id,
            "user_id": username,
            "title": normalized_title,
            "run_count": 0,
            "total_tokens": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "first_recorded_at": None,
            "last_recorded_at": None,
            "items": [],
        },
    }


def _empty_user_token_usage(username: str, sessions: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    normalized_sessions = []
    for session in sessions or []:
        normalized_sessions.append(
            {
                "session_id": str(session.get("session_id") or session.get("agentdo_session_id") or ""),
                "conversation_id": str(session.get("conversation_id") or ""),
                "title": str(session.get("title") or session.get("conversation_id") or ""),
                "workspace_path": str(session.get("workspace_path") or ""),
                "run_count": 0,
                "total_tokens": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0,
                "first_recorded_at": None,
                "last_recorded_at": None,
            }
        )
    return {
        "username": username,
        "tokenUsage": {
            "user_id": username,
            "run_count": 0,
            "session_count": len(normalized_sessions),
            "total_tokens": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "first_recorded_at": None,
            "last_recorded_at": None,
            "sessions": normalized_sessions,
        },
    }


def _trace_log(stage: str, **fields: Any) -> None:
    logger.info(
        "workshop_agentdo %s",
        json.dumps({"stage": stage, **fields}, ensure_ascii=False, default=str),
    )

def _join_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def _request_public_base_url(request: Request) -> str:
    forwarded_proto = str(request.headers.get("x-forwarded-proto") or "").strip()
    forwarded_host = str(request.headers.get("x-forwarded-host") or "").strip()
    if forwarded_proto and forwarded_host:
        return f"{forwarded_proto}://{forwarded_host}/"

    origin = str(request.headers.get("origin") or "").strip()
    if origin:
        return f"{origin.rstrip('/')}/"

    return str(request.base_url)


def _public_preview_url(request: Request, username: str, conversation_id: str) -> str:
    quoted_username = urllib_parse.quote(username)
    quoted_conversation = urllib_parse.quote(conversation_id)
    preview_path = f"{WORKSHOP_PUBLIC_API_BASE}/agent-do/preview/{quoted_username}/{quoted_conversation}"
    return _join_url(_request_public_base_url(request), preview_path)


def _public_runtime_url(request: Request, runtime: dict[str, Any]) -> str | None:
    host_port = runtime.get("host_port")
    if not host_port:
        return None

    parsed = urllib_parse.urlparse(_request_public_base_url(request))
    scheme = parsed.scheme or "http"
    host = parsed.hostname or "localhost"
    return f"{scheme}://{host}:{int(host_port)}/"


def _resolve_workspace_path(mapping: dict[str, Any] | None) -> Path:
    if not mapping:
        raise HTTPException(status_code=404, detail="Conversation mapping not found")
    workspace_path = Path(str(mapping.get("workspace_path") or "")).resolve()
    if not str(workspace_path):
        raise HTTPException(status_code=404, detail="Workspace path is missing")
    if not workspace_path.exists() or not workspace_path.is_dir():
        raise HTTPException(status_code=503, detail="Workspace path is unavailable")
    return workspace_path


def _safe_workspace_target(workspace_path: Path, relative_path: str) -> Path:
    normalized = str(relative_path or "").strip().replace("\\", "/").lstrip("/")
    if not normalized:
        raise HTTPException(status_code=400, detail="Path is required")
    target = (workspace_path / normalized).resolve()
    try:
        target.relative_to(workspace_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid workspace path") from exc
    return target


def _sanitize_workspace_filename(name: str) -> str:
    normalized = str(name or "").strip().replace("\\", "/").split("/")[-1]
    if not normalized:
        normalized = "upload"
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", normalized).strip("._")
    return safe or "upload"


def _allocate_workspace_upload_path(workspace_path: Path, original_name: str) -> tuple[Path, str, str]:
    uploads_dir = (workspace_path / "uploads").resolve()
    uploads_dir.mkdir(parents=True, exist_ok=True)

    safe_name = _sanitize_workspace_filename(original_name)
    stem = Path(safe_name).stem or "upload"
    suffix = Path(safe_name).suffix
    candidate = uploads_dir / safe_name
    index = 2
    while candidate.exists():
        candidate = uploads_dir / f"{stem}-{index}{suffix}"
        index += 1

    relative_path = str(candidate.relative_to(workspace_path)).replace("\\", "/")
    return candidate, candidate.name, relative_path


def _extract_text_path_for_upload(workspace_path: Path, safe_name: str) -> tuple[Path, str]:
    extracted_dir = (workspace_path / "uploads" / "_extracted").resolve()
    extracted_dir.mkdir(parents=True, exist_ok=True)
    extracted_name = f"{safe_name}.txt"
    target = (extracted_dir / extracted_name).resolve()
    relative_path = str(target.relative_to(workspace_path)).replace("\\", "/")
    return target, relative_path


def _decode_text_bytes(raw: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "gb18030"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _normalize_extracted_text(text: str) -> str:
    normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    normalized = normalized.strip()
    if len(normalized) > ATTACHMENT_EXTRACTION_MAX_CHARS:
        normalized = normalized[:ATTACHMENT_EXTRACTION_MAX_CHARS].rstrip()
    return normalized


def _extract_docx_text(raw: bytes) -> str:
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs: list[str] = []
    with zipfile.ZipFile(io.BytesIO(raw)) as docx_zip:
        xml_names: list[str] = []
        for name in docx_zip.namelist():
            normalized_name = str(name).replace("\\", "/")
            if (
                normalized_name == "word/document.xml"
                or normalized_name.startswith("word/header")
                or normalized_name.startswith("word/footer")
                or normalized_name in {"word/footnotes.xml", "word/endnotes.xml"}
            ):
                xml_names.append(name)
        for xml_name in xml_names:
            try:
                root = ET.fromstring(docx_zip.read(xml_name))
            except Exception:
                continue
            for paragraph in root.findall(".//w:p", namespace):
                runs = [
                    "".join(text_node.itertext()).strip()
                    for text_node in paragraph.findall(".//w:t", namespace)
                ]
                content = "".join(part for part in runs if part)
                if content:
                    paragraphs.append(content)
    return "\n".join(paragraphs)


def _extract_pdf_text(raw: bytes) -> str:
    if PdfReader is None:
        raise RuntimeError("PDF text extraction is unavailable because pypdf is not installed")
    reader = PdfReader(io.BytesIO(raw))
    pages: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            pages.append(text.strip())
    return "\n\n".join(pages)


def _extract_uploaded_document_text(raw: bytes, safe_name: str, content_type: str) -> tuple[str, str, str]:
    extension = Path(safe_name).suffix.lower()
    normalized_content_type = str(content_type or "").strip().lower()
    try:
        if extension == ".docx":
            text = _extract_docx_text(raw)
        elif extension == ".pdf":
            text = _extract_pdf_text(raw)
        elif normalized_content_type.startswith("text/") or extension in TEXT_ATTACHMENT_EXTENSIONS:
            text = _decode_text_bytes(raw)
        else:
            return "failed", "", f"暂不支持解析 {extension or normalized_content_type or '该文件类型'}"
    except Exception as exc:
        return "failed", "", str(exc)

    normalized = _normalize_extracted_text(text)
    if not normalized:
        return "empty", "", ""
    return "success", normalized, ""


def _build_uploaded_file_payload(
    *,
    original_name: str,
    safe_name: str,
    relative_path: str,
    content_type: str,
    size: int,
    extracted_path: str = "",
    extraction_status: str | None = None,
    extracted_chars: int = 0,
    error: str = "",
) -> dict[str, Any]:
    attachment_type = "image" if str(content_type or "").lower().startswith("image/") else "document"
    resolved_status = extraction_status or ("not_applicable" if attachment_type == "image" else "success")
    uploaded_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {
        "id": uuid.uuid4().hex,
        "attachmentType": attachment_type,
        "originalName": str(original_name or "").strip() or safe_name,
        "safeName": safe_name,
        "extension": Path(safe_name).suffix.lower(),
        "contentType": str(content_type or "").strip(),
        "size": int(size or 0),
        "uploadedAt": uploaded_at,
        "originalPath": relative_path,
        "extractedPath": extracted_path,
        "extractionStatus": resolved_status,
        "extractedChars": int(extracted_chars or 0),
        "error": str(error or "").strip(),
    }


def _agentdo_json_request(
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: int = 120,
) -> Any:
    if not AGENT_DO_BASE_URL:
        raise RuntimeError("Agent-Do base url is not configured")

    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib_request.Request(
        f"{AGENT_DO_BASE_URL}{path}",
        data=body,
        method=method.upper(),
        headers=headers,
    )

    try:
        with urllib_request.urlopen(req, timeout=timeout) as res:
            raw = res.read()
            if not raw:
                return None
            return json.loads(raw.decode("utf-8"))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Agent-Do HTTP {exc.code}: {detail}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Agent-Do unreachable: {exc.reason}") from exc


def _agentdo_get_session(agentdo_session_id: str) -> dict[str, Any] | None:
    try:
        return _agentdo_json_request("GET", f"/sessions/{agentdo_session_id}", timeout=20)
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return None
        raise


def _ensure_agentdo_session(
    payload: AgentDoGenerateRequest | AgentDoSessionEnsureRequest,
) -> dict[str, Any]:
    mapping = _get_session_mapping(payload.username, payload.conversation_id)
    if mapping:
        session = _agentdo_get_session(mapping["agentdo_session_id"])
        if session:
            workspace_path = str(session.get("workspace_path") or mapping.get("workspace_path") or "")
            _save_session_mapping(
                payload.username,
                payload.conversation_id,
                mapping["agentdo_session_id"],
                workspace_path,
            )
            return session

    session = _agentdo_json_request(
        "POST",
        "/sessions",
        payload={
            "user_id": payload.username,
            "title": payload.title or payload.conversation_id,
        },
        timeout=60,
    )
    _save_session_mapping(
        payload.username,
        payload.conversation_id,
        str(session["id"]),
        str(session.get("workspace_path") or ""),
    )
    return session


def _normalize_generation_mode(mode: str | None) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized in {"vite", "single_html"}:
        return normalized
    return "single_html"


def _build_mode_system_prompt(mode: str) -> str:
    if mode == "vite":
        return (
            "Use a Vite multi-file project for this request. "
            "Create a proper package.json, index.html, and src files. "
            "Do not collapse the app into a single HTML file. "
            "Organize the code for maintainability and future iteration. "
            "Before finishing, verify that /workspace/package.json exists and contains a runnable dev or start script."
        )
    return (
        "For pure frontend requests, create a single /workspace/index.html with inline CSS/JS. "
        "Do not create an npm or Vite project unless the user explicitly asks for a framework. "
        "Before finishing, verify that /workspace/index.html exists in the workspace."
    )


def _build_agentdo_message_payload(payload: AgentDoGenerateRequest) -> dict[str, Any]:
    generation_mode = _normalize_generation_mode(payload.generation_mode)
    append_system_prompt = "\n\n".join(
        part.strip()
        for part in [
            payload.system_prompt or "",
            _build_mode_system_prompt(generation_mode),
        ]
        if part and part.strip()
    )
    mode_instruction = (
        "Build the frontend as a Vite multi-file project with a clean src structure."
        if generation_mode == "vite"
        else "Create a single /workspace/index.html with inline CSS/JS for pure frontend work."
    )
    execution_prompt = (
        "Act directly in the workspace. Do not ask follow-up questions when the request is already actionable. "
        "Create or modify files yourself instead of only describing code. "
        "Do not stop at a text answer; the final state must include the required project files in /workspace. "
        f"{mode_instruction}\n\n"
        f"User request:\n{payload.context}"
    )
    return {
        "content": execution_prompt,
        "model": AGENT_DO_DEFAULT_MODEL,
        "max_turns": AGENT_DO_MAX_TURNS,
        "append_system_prompt": append_system_prompt or None,
    }


def _build_agentdo_acceptance_payload(payload: AgentDoGenerateRequest) -> dict[str, Any]:
    message_payload = _build_agentdo_message_payload(payload)
    return {
        "original_prompt": payload.context,
        "generation_mode": _normalize_generation_mode(payload.generation_mode),
        "model": message_payload["model"],
        "max_turns": message_payload["max_turns"],
        "append_system_prompt": message_payload.get("append_system_prompt"),
        "auto_repair_rounds": max(0, AGENT_DO_AUTO_REPAIR_ROUNDS),
    }


def _is_preview_bootstrap_error(message: str) -> bool:
    detail = str(message or "")
    return any(
        marker in detail
        for marker in (
            "当前 session 没有可在线运行的项目",
            "没有可运行的 dev/start 脚本",
            "没有可预览",
        )
    )


def _build_preview_bootstrap_repair_payload(payload: AgentDoGenerateRequest) -> dict[str, Any]:
    generation_mode = _normalize_generation_mode(payload.generation_mode)
    repair_lines = [
        "The previous step did not leave a previewable project in /workspace.",
        "Do not answer with explanation only. Create or fix the actual files now.",
        "Verify the required files exist before you finish.",
        "",
        f"Original user request:\n{payload.context}",
        "",
        "Required result:",
    ]
    if generation_mode == "vite":
        repair_lines.extend(
            [
                "- A runnable Vite project under /workspace.",
                "- /workspace/package.json with a usable dev or start script.",
                "- /workspace/index.html.",
                "- /workspace/src/main.js or an equivalent entry file.",
                "- Keep the implementation lightweight unless the user explicitly requested a heavy framework.",
            ]
        )
    else:
        repair_lines.extend(
            [
                "- /workspace/index.html must exist.",
                "- Keep CSS and JS inline or as local files inside /workspace.",
                "- Do not switch to Vite or another framework unless the user explicitly asked for it.",
            ]
        )

    repair_payload = _build_agentdo_message_payload(payload)
    repair_payload["content"] = "\n".join(repair_lines).strip()
    return repair_payload


def _repair_agentdo_preview_project(agentdo_session_id: str, payload: AgentDoGenerateRequest) -> dict[str, Any]:
    return _agentdo_json_request(
        "POST",
        f"/sessions/{agentdo_session_id}/messages",
        payload=_build_preview_bootstrap_repair_payload(payload),
        timeout=900,
    ) or {}


def _start_agentdo_runtime_with_recovery(agentdo_session_id: str, payload: AgentDoGenerateRequest) -> dict[str, Any]:
    try:
        return _start_agentdo_runtime(agentdo_session_id)
    except RuntimeError as exc:
        if not _is_preview_bootstrap_error(str(exc)):
            raise
        _repair_agentdo_preview_project(agentdo_session_id, payload)
        return _start_agentdo_runtime(agentdo_session_id)


def _start_agentdo_runtime(agentdo_session_id: str) -> dict[str, Any]:
    return _agentdo_json_request(
        "POST",
        f"/sessions/{agentdo_session_id}/runtime/start",
        payload={"restart": False},
        timeout=180,
    ) or {}


def _stop_agentdo_claude_runtime(agentdo_session_id: str) -> dict[str, Any] | None:
    if not AGENT_DO_RELEASE_CLAUDE_AFTER_REQUEST:
        return None
    try:
        return _agentdo_json_request(
            "POST",
            f"/sessions/{agentdo_session_id}/claude-runtime/stop",
            timeout=30,
        ) or {}
    except Exception as exc:
        logger.warning("failed to stop Agent-Do Claude runtime %s: %s", agentdo_session_id, exc)
        return None


def _run_agentdo_acceptance(agentdo_session_id: str, payload: AgentDoGenerateRequest) -> dict[str, Any]:
    return _agentdo_json_request(
        "POST",
        f"/sessions/{agentdo_session_id}/acceptance",
        payload=_build_agentdo_acceptance_payload(payload),
        timeout=AGENT_DO_ACCEPTANCE_TIMEOUT,
    ) or {}


def _build_preview_result(
    request: Request,
    payload: AgentDoGenerateRequest,
    session: dict[str, Any],
    runtime: dict[str, Any],
    acceptance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    preview_url = (
        _public_runtime_url(request, runtime)
        if runtime.get("mode") == "node" and runtime.get("host_port")
        else _public_preview_url(request, payload.username, payload.conversation_id)
    )
    preview_ready = bool(runtime.get("can_preview") or runtime.get("preview_url") or runtime.get("mode") == "static")
    return {
        "strategy": "agent-do",
        "url": preview_url,
        "previewReady": preview_ready,
        "conversationId": payload.conversation_id,
        "agentDoSessionId": session.get("id"),
        "workspacePath": session.get("workspace_path"),
        "port": runtime.get("host_port"),
        "upstreamUrlPath": runtime.get("preview_url"),
        "runtimeMode": runtime.get("mode"),
        "runtimeStatus": runtime.get("status"),
        "acceptance": acceptance or None,
    }


async def _generate_with_agentdo(
    payload: AgentDoGenerateRequest, request: Request, request_id: str
) -> dict[str, Any]:
    trace = Trace(
        "agentdo/generate-preview",
        request_id=request_id,
        meta={"conversationId": payload.conversation_id, "username": payload.username},
    )
    started = time.perf_counter()
    _trace_log(
        "generate.start",
        requestId=request_id,
        conversationId=payload.conversation_id,
        username=payload.username,
        title=payload.title,
        context=payload.context,
        systemPrompt=payload.system_prompt,
    )

    session: dict[str, Any] | None = None
    try:
        health_started = time.perf_counter()
        with trace.step("health_check"):
            await asyncio.to_thread(_agentdo_json_request, "GET", "/healthz", timeout=10)
        _trace_log(
            "generate.health.ok",
            requestId=request_id,
            elapsedMs=round((time.perf_counter() - health_started) * 1000),
        )

        with trace.step("session_resolve"):
            session = await asyncio.to_thread(_ensure_agentdo_session, payload)

        message_started = time.perf_counter()
        with trace.step("upstream_generate"):
            await asyncio.to_thread(
                _agentdo_json_request,
                "POST",
                f"/sessions/{session['id']}/messages",
                payload=_build_agentdo_message_payload(payload),
                timeout=900,
            )
        _trace_log(
            "generate.upstream.ok",
            requestId=request_id,
            elapsedMs=round((time.perf_counter() - message_started) * 1000),
            agentDoSessionId=session["id"],
        )

        runtime_started = time.perf_counter()
        with trace.step("preview_start"):
            runtime = await asyncio.to_thread(
                _start_agentdo_runtime_with_recovery,
                str(session["id"]),
                payload,
            )
        _trace_log(
            "generate.preview.ready",
            requestId=request_id,
            elapsedMs=round((time.perf_counter() - runtime_started) * 1000),
            runtime=runtime,
        )

        acceptance_started = time.perf_counter()
        with trace.step("browser_acceptance"):
            acceptance = await asyncio.to_thread(_run_agentdo_acceptance, str(session["id"]), payload)
        _trace_log(
            "generate.acceptance.done",
            requestId=request_id,
            elapsedMs=round((time.perf_counter() - acceptance_started) * 1000),
            acceptance=acceptance,
        )

        with trace.step("build_response"):
            response_payload = _build_preview_result(request, payload, session, acceptance.get("runtime") or runtime, acceptance)
        _trace_log(
            "generate.done",
            requestId=request_id,
            elapsedMs=round((time.perf_counter() - started) * 1000),
            response=response_payload,
        )

        trace.finish()
        return response_payload
    except Exception as exc:
        trace.finish(error=str(exc))
        raise
    finally:
        if session and session.get("id"):
            await asyncio.to_thread(_stop_agentdo_claude_runtime, str(session["id"]))


def _stream_agentdo_sse(
    payload: AgentDoGenerateRequest, request: Request, request_id: str
):
    if not AGENT_DO_BASE_URL:
        error_payload = json.dumps(
            {"type": "error", "content": "Agent-Do base url is not configured"},
            ensure_ascii=False,
        )
        _trace_log("stream.config.error", requestId=request_id, detail="Agent-Do base url is not configured")
        yield f"data: {error_payload}\n\n"
        return

    trace = Trace(
        "agentdo/generate-preview/stream",
        request_id=request_id,
        meta={"conversationId": payload.conversation_id, "username": payload.username},
    )
    started = time.perf_counter()
    _trace_log(
        "stream.start",
        requestId=request_id,
        conversationId=payload.conversation_id,
        username=payload.username,
        title=payload.title,
        context=payload.context,
        systemPrompt=payload.system_prompt,
    )

    session: dict[str, Any] | None = None
    with trace.step("session_resolve"):
        session = _ensure_agentdo_session(payload)

    body = json.dumps(_build_agentdo_message_payload(payload)).encode("utf-8")
    req = urllib_request.Request(
        f"{AGENT_DO_BASE_URL}/sessions/{session['id']}/messages/stream",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        },
    )

    intro = {
        "type": "meta",
        "conversationId": payload.conversation_id,
        "agentDoSessionId": session.get("id"),
        "workspacePath": session.get("workspace_path"),
    }
    yield f"data: {json.dumps(intro, ensure_ascii=False)}\n\n"
    session_line = (
        "Agent-Do session established; streaming Claude output via "
        f"POST /sessions/{session['id']}/messages/stream (SSE)"
    )
    yield f"data: {json.dumps({'type': 'status', 'stage': 'session', 'content': session_line}, ensure_ascii=False)}\n\n"

    connect_t0 = time.perf_counter()
    try:
        event_count = 0
        first_event_recorded = False
        with urllib_request.urlopen(req, timeout=900) as res:
            trace.record_step(
                "upstream_connect",
                round((time.perf_counter() - connect_t0) * 1000, 2),
            )
            stream_t0 = time.perf_counter()
            current_event = ""
            data_lines: list[str] = []

            def flush_agentdo_event() -> list[dict[str, Any]]:
                nonlocal current_event, data_lines
                if not data_lines:
                    current_event = ""
                    return []
                raw = "\n".join(data_lines).strip()
                event_name = current_event or "message"
                current_event = ""
                data_lines = []
                if not raw:
                    return []
                try:
                    source = json.loads(raw)
                except ValueError:
                    return []

                if event_name == "started":
                    return [{
                        "type": "status",
                        "stage": "generate",
                        "content": f"Claude stream started, session_id={source.get('session_id', session['id'])}",
                    }]
                if event_name == "chunk":
                    text_value = str(source.get("text") or "")
                    return [{
                        "type": "delta",
                        "partType": "text",
                        "content": text_value,
                    }] if text_value else []
                if event_name == "tool":
                    title = str(source.get("title") or source.get("tool") or "tool")
                    return [{
                        "type": "tool",
                        "tool": str(source.get("tool") or title),
                        "title": title,
                        "status": str(source.get("status") or "running"),
                        "input": str(source.get("input") or ""),
                        "output": str(source.get("output") or ""),
                        "error": str(source.get("error") or ""),
                    }]
                if event_name == "done":
                    return [{
                        "type": "status",
                        "stage": "preview",
                        "content": "Claude output received; starting preview runtime",
                    }]
                if event_name == "ping":
                    return [{
                        "type": "ping",
                        "timestamp": source.get("timestamp"),
                    }]
                if event_name == "error":
                    return [{
                        "type": "error",
                        "content": str(source.get("message") or "Agent-Do stream failed"),
                    }]
                return []

            def emit_items(items: list[dict[str, Any]]):
                nonlocal event_count, first_event_recorded
                for item in items:
                    if not first_event_recorded:
                        first_event_recorded = True
                        trace.record_step(
                            "stream_first_event",
                            round((time.perf_counter() - stream_t0) * 1000, 2),
                        )
                    try:
                        record_stream_event(
                            request_id,
                            event_count + 1,
                            item.get("type", "unknown"),
                            json.dumps(item, ensure_ascii=False)[:200],
                            item,
                        )
                    except Exception:
                        pass
                    _trace_log(
                        "stream.event",
                        requestId=request_id,
                        elapsedMs=round((time.perf_counter() - started) * 1000),
                        index=event_count + 1,
                        event=item,
                    )
                    event_count += 1
                    yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"

            for raw_line in res:
                line = raw_line.decode("utf-8", errors="replace")
                stripped = line.strip("\r\n")
                if not stripped:
                    for output_line in emit_items(flush_agentdo_event()):
                        yield output_line
                    continue
                if stripped.startswith("event:"):
                    current_event = stripped[len("event:"):].strip()
                    continue
                if stripped.startswith("data:"):
                    data_lines.append(stripped[len("data:"):].strip())

            for output_line in emit_items(flush_agentdo_event()):
                yield output_line

        try:
            with trace.step("preview_start"):
                runtime = _start_agentdo_runtime(str(session["id"]))
        except RuntimeError as exc:
            if not _is_preview_bootstrap_error(str(exc)):
                trace.finish(error=str(exc))
                err_item = {
                    "type": "error",
                    "content": str(exc),
                    "agentDoSessionId": session.get("id"),
                    "workspacePath": session.get("workspace_path"),
                }
                yield f"data: {json.dumps(err_item, ensure_ascii=False)}\n\n"
                return

            recovery_status = {
                "type": "status",
                "stage": "repair",
                "content": "检测到工作区还没有形成可预览项目，正在自动补齐必要文件后重试预览启动...",
            }
            yield f"data: {json.dumps(recovery_status, ensure_ascii=False)}\n\n"
            try:
                with trace.step("preview_materialize"):
                    _repair_agentdo_preview_project(str(session["id"]), payload)
                with trace.step("preview_restart"):
                    runtime = _start_agentdo_runtime(str(session["id"]))
            except RuntimeError as retry_exc:
                trace.finish(error=str(retry_exc))
                err_item = {
                    "type": "error",
                    "content": str(retry_exc),
                    "agentDoSessionId": session.get("id"),
                    "workspacePath": session.get("workspace_path"),
                }
                yield f"data: {json.dumps(err_item, ensure_ascii=False)}\n\n"
                return

        acceptance_intro = {
            "type": "status",
            "stage": "acceptance",
            "content": "Preview runtime is up; running Playwright browser acceptance",
        }
        yield f"data: {json.dumps(acceptance_intro, ensure_ascii=False)}\n\n"

        try:
            with trace.step("browser_acceptance"):
                acceptance = _run_agentdo_acceptance(str(session["id"]), payload)
        except RuntimeError as exc:
            trace.finish(error=str(exc))
            err_item = {
                "type": "error",
                "content": str(exc),
                "agentDoSessionId": session.get("id"),
                "workspacePath": session.get("workspace_path"),
            }
            yield f"data: {json.dumps(err_item, ensure_ascii=False)}\n\n"
            return

        acceptance_status = {
            "type": "status",
            "stage": "acceptance",
            "content": (
                "Playwright acceptance passed"
                if acceptance.get("passed")
                else "Playwright acceptance required repair or still has issues"
            ),
        }
        yield f"data: {json.dumps(acceptance_status, ensure_ascii=False)}\n\n"

        result_item = {
            "type": "result",
            "url": (
                _public_runtime_url(request, acceptance.get("runtime") or runtime)
                if (acceptance.get("runtime") or runtime).get("mode") == "node"
                and (acceptance.get("runtime") or runtime).get("host_port")
                else _public_preview_url(request, payload.username, payload.conversation_id)
            ),
            "agentDoSessionId": session.get("id"),
            "workspacePath": session.get("workspace_path"),
            "runtimeMode": (acceptance.get("runtime") or runtime).get("mode"),
            "runtimeStatus": (acceptance.get("runtime") or runtime).get("status"),
            "acceptance": acceptance,
        }
        yield f"data: {json.dumps(result_item, ensure_ascii=False)}\n\n"
        event_count += 1
        trace.record_step(
            "stream_complete",
            round((time.perf_counter() - stream_t0) * 1000, 2),
            eventCount=event_count,
        )
        _trace_log(
            "stream.done",
            requestId=request_id,
            elapsedMs=round((time.perf_counter() - started) * 1000),
            eventCount=event_count,
        )
        trace.finish()
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        _trace_log(
            "stream.http_error",
            requestId=request_id,
            elapsedMs=round((time.perf_counter() - started) * 1000),
            status=exc.code,
            detail=detail,
        )
        trace.finish(error=f"HTTP {exc.code}: {detail}")
        err_data = json.dumps(
            {"type": "error", "content": f"Agent-Do HTTP {exc.code}: {detail}"},
            ensure_ascii=False,
        )
        yield f"data: {err_data}\n\n"
    except urllib_error.URLError as exc:
        _trace_log(
            "stream.network_error",
            requestId=request_id,
            elapsedMs=round((time.perf_counter() - started) * 1000),
            detail=str(exc.reason),
        )
        trace.finish(error=f"URLError: {exc.reason}")
        err_data = json.dumps(
            {"type": "error", "content": f"Agent-Do unreachable: {exc.reason}"},
            ensure_ascii=False,
        )
        yield f"data: {err_data}\n\n"
    finally:
        if session and session.get("id"):
            _stop_agentdo_claude_runtime(str(session["id"]))


def _filter_proxy_headers(headers: Any) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in HOP_BY_HOP_HEADERS:
            continue
        result[key] = value
    return result


def _forward_proxy_request(
    method: str,
    path: str,
    *,
    headers: dict[str, str],
    body: bytes | None,
    timeout: int = 120,
) -> tuple[int, bytes, dict[str, str]]:
    if not AGENT_DO_BASE_URL:
        raise RuntimeError("Agent-Do base url is not configured")

    upstream_headers = _filter_proxy_headers(headers)

    req = urllib_request.Request(
        f"{AGENT_DO_BASE_URL}{path}",
        data=body,
        method=method.upper(),
        headers=upstream_headers,
    )

    try:
        with urllib_request.urlopen(req, timeout=timeout) as res:
            return res.getcode(), res.read(), _filter_proxy_headers(res.headers)
    except urllib_error.HTTPError as exc:
        return exc.code, exc.read(), _filter_proxy_headers(exc.headers)
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Agent-Do unreachable: {exc.reason}") from exc


_UPLOAD_MAX_SIZE = 10 * 1024 * 1024  # 10 MB


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    request_id = uuid.uuid4().hex[:12]
    trace = Trace("upload", request_id=request_id, meta={"filename": file.filename})

    if not ACCESS_KEY_ID or not ACCESS_KEY_SECRET:
        trace.finish(error="OSS credentials not configured")
        return {"error": "OSS credentials are not configured properly."}

    try:
        with trace.step("read_file"):
            content = await file.read()

        if len(content) > _UPLOAD_MAX_SIZE:
            trace.finish(error="file too large")
            raise HTTPException(status_code=413, detail="File too large (max 10 MB)")

        original = file.filename or "upload.html"
        ext = os.path.splitext(original)[1] or ".html"
        safe_name = f"{uuid.uuid4().hex}{ext}"

        with trace.step("oss_upload", size_bytes=len(content)):
            auth_oss = oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET)
            bucket = oss2.Bucket(auth_oss, ENDPOINT, BUCKET_NAME)
            bucket.put_object(
                safe_name,
                content,
                headers={"Content-Type": "text/html; charset=utf-8"},
            )

        url = f"{DOMAIN}/{safe_name}"
        trace.finish()
        return {"url": url}
    except HTTPException:
        raise
    except Exception as exc:
        trace.finish(error=str(exc))
        raise


@app.post("/agent-do/generate-preview")
async def generate_preview_with_agentdo(payload: AgentDoGenerateRequest, request: Request):
    request_id = uuid.uuid4().hex[:12]
    try:
        return await _generate_with_agentdo(payload, request, request_id)
    except Exception as exc:
        _trace_log(
            "generate.error",
            requestId=request_id,
            conversationId=payload.conversation_id,
            username=payload.username,
            detail=str(exc),
        )
        raise HTTPException(status_code=503, detail=str(exc)) from exc


# [容器池功能暂时禁用]
@app.post("/agent-do/session-mapping/ensure")
async def ensure_agentdo_session_mapping(payload: AgentDoSessionEnsureRequest):
    try:
        session = await asyncio.to_thread(_ensure_agentdo_session, payload)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return _build_session_mapping_response(
        payload.username,
        payload.conversation_id,
        str(session.get("id") or ""),
        str(session.get("workspace_path") or ""),
    )


@app.post("/agent-do/session-mapping/restore")
async def restore_agentdo_session_mapping(payload: AgentDoSessionRestoreRequest):
    try:
        session = await asyncio.to_thread(_agentdo_get_session, payload.agentdo_session_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if not session:
        response = _build_session_mapping_response(
            payload.username,
            payload.conversation_id,
            "",
            "",
        )
        response["restored"] = False
        return response

    workspace_path = str(payload.workspace_path or session.get("workspace_path") or "")
    await asyncio.to_thread(
        _save_session_mapping,
        payload.username,
        payload.conversation_id,
        payload.agentdo_session_id,
        workspace_path,
    )
    response = _build_session_mapping_response(
        payload.username,
        payload.conversation_id,
        payload.agentdo_session_id,
        workspace_path,
    )
    response["restored"] = True
    return response


@app.delete("/agent-do/session-mapping/{username}/{conversation_id}")
async def delete_agentdo_session_mapping(username: str, conversation_id: str):
    deleted_mapping = await asyncio.to_thread(_delete_session_mapping, username, conversation_id)
    response = _build_session_mapping_response(
        username,
        conversation_id,
        str(deleted_mapping.get("agentdo_session_id") if deleted_mapping else ""),
        str(deleted_mapping.get("workspace_path") if deleted_mapping else ""),
    )
    response["deleted"] = bool(deleted_mapping)
    return response


@app.get("/agent-do/tokens/conversation/{username}/{conversation_id}")
async def get_agentdo_conversation_token_usage(username: str, conversation_id: str):
    mapping = await asyncio.to_thread(_get_session_mapping, username, conversation_id)
    if not mapping:
        return _empty_conversation_token_usage(username, conversation_id)

    session_title = conversation_id
    workspace_path = str(mapping.get("workspace_path") or "")
    try:
        session = await asyncio.to_thread(_agentdo_get_session, str(mapping["agentdo_session_id"]))
    except Exception:
        session = None
    if session:
        session_title = str(session.get("title") or session_title)
        workspace_path = str(session.get("workspace_path") or workspace_path)

    return _empty_conversation_token_usage(
        username,
        conversation_id,
        str(mapping.get("agentdo_session_id") or ""),
        workspace_path,
        session_title,
    )


@app.get("/agent-do/tokens/user/{username}")
async def get_agentdo_user_token_usage(username: str):
    mappings = await asyncio.to_thread(_list_session_mappings_for_user, username)
    sessions: list[dict[str, Any]] = []
    for mapping in mappings:
        session_title = str(mapping.get("conversation_id") or "")
        workspace_path = str(mapping.get("workspace_path") or "")
        try:
            session = await asyncio.to_thread(_agentdo_get_session, str(mapping["agentdo_session_id"]))
        except Exception:
            session = None
        if session:
            session_title = str(session.get("title") or session_title)
            workspace_path = str(session.get("workspace_path") or workspace_path)
        sessions.append(
            {
                "session_id": str(mapping.get("agentdo_session_id") or ""),
                "conversation_id": str(mapping.get("conversation_id") or ""),
                "title": session_title,
                "workspace_path": workspace_path,
            }
        )
    return _empty_user_token_usage(username, sessions)


# @app.get("/agent-do/sandbox-pool")
# async def get_agentdo_sandbox_pool():
#     try:
#         sessions = await asyncio.to_thread(_agentdo_json_request, "GET", "/sessions", timeout=30)
#         items = sessions.get("items") if isinstance(sessions, dict) else []
#         active = []
#         for session in items or []:
#             try:
#                 runtime = await asyncio.to_thread(
#                     _agentdo_json_request,
#                     "GET",
#                     f"/sessions/{session['id']}/runtime",
#                     timeout=30,
#                 )
#             except Exception:
#                 continue
#             if runtime.get("can_preview") or runtime.get("status") in {"ready", "running"}:
#                 active.append({
#                     "username": session.get("user_id", ""),
#                     "conversationId": session.get("title") or session.get("id"),
#                     "workspacePath": session.get("workspace_path", ""),
#                     "containerName": runtime.get("container_name") or "",
#                     "host": AGENT_DO_BASE_URL,
#                     "port": runtime.get("host_port") or 0,
#                     "kind": runtime.get("mode") or "",
#                     "language": "node" if runtime.get("mode") == "node" else "html",
#                     "startedAt": 0,
#                     "lastAccessedAt": 0,
#                 })
#         return {
#             "runtimeRoot": "",
#             "activeCount": len(active),
#             "maxContainers": 0,
#             "idleTtlMs": 0,
#             "activeSandboxes": active,
#             "reclaimedSandboxes": [],
#         }
#     except Exception as exc:
#         raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/agent-do/generate-preview/stream")
async def stream_preview_with_agentdo(payload: AgentDoGenerateRequest, request: Request):
    request_id = uuid.uuid4().hex[:12]
    return StreamingResponse(
        _stream_agentdo_sse(payload, request, request_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.api_route(
    "/agent-do/preview/{username}/{conversation_id}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    name="agentdo_preview_root",
)
@app.api_route(
    "/api/workshop/agent-do/preview/{username}/{conversation_id}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    name="agentdo_preview_root_api_prefix",
    include_in_schema=False,
)
@app.api_route(
    "/agent-do/preview/{username}/{conversation_id}/{asset_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    name="agentdo_preview_asset",
)
@app.api_route(
    "/api/workshop/agent-do/preview/{username}/{conversation_id}/{asset_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    name="agentdo_preview_asset_api_prefix",
    include_in_schema=False,
)
async def agentdo_preview_proxy(
    request: Request,
    username: str,
    conversation_id: str,
    asset_path: str = "",
):
    request_id = uuid.uuid4().hex[:12]
    trace = Trace(
        "agentdo/preview-proxy",
        request_id=request_id,
        meta={"method": request.method, "username": username, "conversationId": conversation_id, "asset": asset_path},
    )

    suffix = f"/{asset_path}" if asset_path else ""
    mapping = _get_session_mapping(username, conversation_id)
    if not mapping:
        trace.finish(error="Conversation mapping not found")
        raise HTTPException(status_code=404, detail="Conversation mapping not found")
    upstream_path = f"/sessions/{urllib_parse.quote(mapping['agentdo_session_id'])}/preview{suffix}"
    if request.url.query:
        upstream_path = f"{upstream_path}?{request.url.query}"

    body = None if request.method in {"GET", "HEAD"} else await request.body()

    try:
        with trace.step("proxy_forward"):
            status_code, content, headers = await asyncio.to_thread(
                _forward_proxy_request,
                request.method,
                upstream_path,
                headers=dict(request.headers.items()),
                body=body,
                timeout=300,
            )
    except RuntimeError as exc:
        trace.finish(error=str(exc))
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    trace.finish()
    return Response(
        content=content,
        status_code=status_code,
        headers=headers,
    )


@app.get("/agent-do/files/{username}/{conversation_id}/tree")
async def agentdo_workspace_tree(username: str, conversation_id: str):
    mapping = _get_session_mapping(username, conversation_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="Conversation mapping not found")
    try:
        return await asyncio.to_thread(
            _agentdo_json_request,
            "GET",
            f"/sessions/{urllib_parse.quote(mapping['agentdo_session_id'])}/workspace/tree",
            timeout=60,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/agent-do/files/{username}/{conversation_id}/content")
async def agentdo_workspace_file(username: str, conversation_id: str, path: str):
    mapping = _get_session_mapping(username, conversation_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="Conversation mapping not found")
    quoted_path = urllib_parse.quote(path, safe="/")
    try:
        return await asyncio.to_thread(
            _agentdo_json_request,
            "GET",
            f"/sessions/{urllib_parse.quote(mapping['agentdo_session_id'])}/workspace/file?path={quoted_path}",
            timeout=60,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/agent-do/files/{username}/{conversation_id}/download")
async def agentdo_workspace_file_download(username: str, conversation_id: str, path: str):
    mapping = _get_session_mapping(username, conversation_id)
    workspace_path = _resolve_workspace_path(mapping)
    target = _safe_workspace_target(workspace_path, path)
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(target, filename=target.name)


@app.post("/agent-do/files/{username}/{conversation_id}/upload")
async def agentdo_workspace_file_upload(
    username: str,
    conversation_id: str,
    request: Request,
    files: list[UploadFile] = File(...),
):
    mapping = _get_session_mapping(username, conversation_id)
    workspace_path = _resolve_workspace_path(mapping)

    normalized_files = [file for file in (files or []) if file is not None]
    if not normalized_files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    uploaded_items: list[dict[str, Any]] = []
    for file in normalized_files:
        original_name = str(file.filename or "").strip() or "upload"
        content_type = str(file.content_type or "")
        raw = await file.read()
        target_path, safe_name, relative_path = _allocate_workspace_upload_path(workspace_path, original_name)
        target_path.write_bytes(raw)
        extracted_path = ""
        extraction_status = "not_applicable" if content_type.lower().startswith("image/") else "success"
        extracted_chars = 0
        extraction_error = ""
        if extraction_status != "not_applicable":
            extraction_status, extracted_text, extraction_error = _extract_uploaded_document_text(
                raw,
                safe_name,
                content_type,
            )
            if extraction_status == "success":
                extracted_target, extracted_path = _extract_text_path_for_upload(workspace_path, safe_name)
                extracted_target.write_text(extracted_text, encoding="utf-8")
                extracted_chars = len(extracted_text)
        uploaded_items.append(
            _build_uploaded_file_payload(
                original_name=original_name,
                safe_name=safe_name,
                relative_path=relative_path,
                content_type=content_type,
                size=len(raw),
                extracted_path=extracted_path,
                extraction_status=extraction_status,
                extracted_chars=extracted_chars,
                error=extraction_error,
            )
        )

    response_payload = {
        **_build_session_mapping_response(
            username,
            conversation_id,
            str(mapping.get("agentdo_session_id") or ""),
            str(mapping.get("workspace_path") or ""),
        ),
        "files": uploaded_items,
        "count": len(uploaded_items),
    }
    return JSONResponse(response_payload)


@app.post("/generate")
async def generate_html_stream(request: GenerateRequest):
    if not DASHSCOPE_API_KEY:
        return JSONResponse(
            {"detail": "DashScope API Key is not configured."},
            status_code=503,
        )

    request_id = uuid.uuid4().hex[:12]

    strict_system_prompt = (
        f"{request.system_prompt}\n\n"
        "【输出格式，必须严格遵守】\n"
        "1. 不要输出任何额外说明、标题、注释或 Markdown 包裹内容。\n"
        "2. 单独占一行，输出且仅输出以下分隔符字符串：\n"
        f"{HTML_BEGIN_MARKER}\n"
        "3. 在该行之后，只输出完整、可独立运行的 HTML 文档源码，不要使用 Markdown 代码块。"
        "HTML 文档结束后不要再输出任何文字。"
    )

    messages = [
        {"role": "system", "content": strict_system_prompt},
        {"role": "user", "content": request.context},
    ]

    async def stream_generator():
        trace = Trace("generate", request_id=request_id)
        try:
            llm_t0 = time.perf_counter()
            client = AsyncOpenAI(api_key=DASHSCOPE_API_KEY, base_url=DASHSCOPE_BASE_URL)
            with trace.step("llm_connect"):
                llm_stream = await client.chat.completions.create(
                    model="qwen-plus",
                    messages=messages,
                    stream=True,
                )

            splitter = _StreamPhaseSplitter()
            first_token = False
            token_t0 = time.perf_counter()

            async for chunk in llm_stream:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                if not delta.content:
                    continue

                if not first_token:
                    first_token = True
                    trace.record_step(
                        "llm_first_token",
                        round((time.perf_counter() - token_t0) * 1000, 2),
                    )

                piece = delta.content
                for kind, text in splitter.feed(piece):
                    if not text:
                        continue
                    event_type = "friendly" if kind == "friendly" else "text"
                    payload = json.dumps({"type": event_type, "content": text}, ensure_ascii=False)
                    yield f"data: {payload}\n\n"
                    await asyncio.sleep(0)

            for kind, text in splitter.flush():
                if not text:
                    continue
                event_type = "friendly" if kind == "friendly" else "text"
                payload = json.dumps({"type": event_type, "content": text}, ensure_ascii=False)
                yield f"data: {payload}\n\n"
                await asyncio.sleep(0)

            trace.record_step(
                "stream_transfer",
                round((time.perf_counter() - token_t0) * 1000, 2),
            )
            yield "data: [DONE]\n\n"
            trace.finish()
        except Exception as exc:
            trace.finish(error=str(exc))
            raise

    return StreamingResponse(
        stream_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/generate-text")
async def generate_text_stream(request: GenerateRequest):
    if not DASHSCOPE_API_KEY:
        return JSONResponse(
            {"detail": "DashScope API Key is not configured."},
            status_code=503,
        )

    request_id = uuid.uuid4().hex[:12]
    messages = [
        {"role": "system", "content": request.system_prompt},
        {"role": "user", "content": request.context},
    ]

    async def stream_generator():
        trace = Trace("generate_text", request_id=request_id)
        try:
            client = AsyncOpenAI(api_key=DASHSCOPE_API_KEY, base_url=DASHSCOPE_BASE_URL)
            token_t0 = time.perf_counter()
            first_token = False
            with trace.step("llm_connect"):
                llm_stream = await client.chat.completions.create(
                    model="qwen-plus",
                    messages=messages,
                    stream=True,
                )

            async for chunk in llm_stream:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                if not delta.content:
                    continue

                if not first_token:
                    first_token = True
                    trace.record_step(
                        "llm_first_token",
                        round((time.perf_counter() - token_t0) * 1000, 2),
                    )

                payload = json.dumps(
                    {"type": "friendly", "content": delta.content},
                    ensure_ascii=False,
                )
                yield f"data: {payload}\n\n"
                await asyncio.sleep(0)

            trace.record_step(
                "stream_transfer",
                round((time.perf_counter() - token_t0) * 1000, 2),
            )
            yield "data: [DONE]\n\n"
            trace.finish()
        except Exception as exc:
            trace.finish(error=str(exc))
            raise

    return StreamingResponse(
        stream_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
