import io
import json
import os
import re
import threading
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field


skill_router = APIRouter(tags=["Workshop Skills"])

SKILL_DATA_ROOT = Path(
    os.getenv(
        "WORKSHOP_SKILL_DATA_DIR",
        str(Path(__file__).resolve().parent / "state" / "skills_repo"),
    )
).resolve()
SKILL_LOCAL_ZIP_ROOT = Path(
    os.getenv(
        "WORKSHOP_SKILL_LOCAL_ZIP_DIR",
        str(Path(__file__).resolve().parent / "state" / "skill_zips"),
    )
).resolve()
SKILL_INDEX_PATH = SKILL_DATA_ROOT / "index.json"
SKILL_MODE_SCOPES = {"skill_assistant", "workshop", "both"}
_STORE_LOCK = threading.Lock()


class SkillCreatePayload(BaseModel):
    name: str
    slug: str = ""
    description: str = ""
    version: str = "1.0.0"
    markdown: str
    changelog: str = ""


class SkillImportLocalZipPayload(BaseModel):
    filename: str
    name: str
    slug: str = ""
    description: str = ""
    version: str = "1.0.0"
    changelog: str = ""


class SkillStatusPayload(BaseModel):
    is_active: bool


class SkillMetaPayload(BaseModel):
    name: str | None = None
    description: str | None = None


class SkillRoutingPayload(BaseModel):
    priority: int | None = None
    routing_tags: list[str] | None = None
    trigger_keywords: list[str] | None = None
    exclude_keywords: list[str] | None = None
    mode_scope: str | None = None


class SkillResolvePayload(BaseModel):
    input: str = ""
    mode: str = "skill_assistant"
    manual_skill_ids: list[str] = Field(default_factory=list)
    max_count: int = 3


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ensure_storage() -> None:
    SKILL_DATA_ROOT.mkdir(parents=True, exist_ok=True)
    SKILL_LOCAL_ZIP_ROOT.mkdir(parents=True, exist_ok=True)


def _load_index() -> dict[str, Any]:
    _ensure_storage()
    if not SKILL_INDEX_PATH.exists():
        return {"skills": []}
    try:
        raw = json.loads(SKILL_INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"skills": []}
    if not isinstance(raw, dict):
        return {"skills": []}
    skills = raw.get("skills")
    if not isinstance(skills, list):
        raw["skills"] = []
    return raw


def _save_index(index_data: dict[str, Any]) -> None:
    _ensure_storage()
    SKILL_INDEX_PATH.write_text(
        json.dumps(index_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _normalize_slug(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    if text:
        return text[:80]
    return f"skill-{uuid4().hex[:8]}"


def _normalize_mode_scope(value: str | None) -> str:
    normalized = str(value or "skill_assistant").strip().lower()
    if normalized not in SKILL_MODE_SCOPES:
        return "skill_assistant"
    return normalized


def _normalize_string_list(value: list[str] | None) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item or "").strip() for item in value if str(item or "").strip()]


def _get_latest_version_record(skill: dict[str, Any]) -> dict[str, Any] | None:
    versions = skill.get("versions")
    if not isinstance(versions, list) or not versions:
        return None
    return versions[-1]


def _serialize_skill_summary(skill: dict[str, Any]) -> dict[str, Any]:
    latest = _get_latest_version_record(skill) or {}
    return {
        "id": str(skill.get("id") or ""),
        "name": str(skill.get("name") or ""),
        "slug": str(skill.get("slug") or ""),
        "description": str(skill.get("description") or ""),
        "latest_version": str(latest.get("version") or ""),
        "is_active": bool(skill.get("is_active", True)),
        "priority": int(skill.get("priority", 50) or 50),
        "mode_scope": _normalize_mode_scope(skill.get("mode_scope")),
        "routing_tags": _normalize_string_list(skill.get("routing_tags")),
        "trigger_keywords": _normalize_string_list(skill.get("trigger_keywords")),
        "exclude_keywords": _normalize_string_list(skill.get("exclude_keywords")),
        "version_count": len(skill.get("versions") or []),
        "created_at": str(skill.get("created_at") or ""),
        "updated_at": str(skill.get("updated_at") or ""),
        "deleted_at": skill.get("deleted_at"),
    }


def _serialize_skill_detail(skill: dict[str, Any]) -> dict[str, Any]:
    data = _serialize_skill_summary(skill)
    versions = []
    for item in skill.get("versions") or []:
        versions.append(
            {
                "version": str(item.get("version") or ""),
                "entry_file": str(item.get("entry_file") or "SKILL.md"),
                "reference_count": int(item.get("reference_count") or 0),
                "created_at": str(item.get("created_at") or ""),
                "changelog": str(item.get("changelog") or ""),
            }
        )
    data["versions"] = versions
    return data


def _serialize_skill_version(skill: dict[str, Any], version_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "skill_id": str(skill.get("id") or ""),
        "name": str(skill.get("name") or ""),
        "slug": str(skill.get("slug") or ""),
        "version": str(version_record.get("version") or ""),
        "markdown": str(version_record.get("markdown") or ""),
        "entry_file": str(version_record.get("entry_file") or "SKILL.md"),
        "reference_count": int(version_record.get("reference_count") or 0),
        "manifest": version_record.get("manifest") or {"references": []},
        "created_at": str(version_record.get("created_at") or ""),
        "changelog": str(version_record.get("changelog") or ""),
    }


def _find_skill(index_data: dict[str, Any], skill_id: str) -> dict[str, Any] | None:
    for skill in index_data.get("skills") or []:
        if str(skill.get("id") or "") == skill_id:
            return skill
    return None


def _find_skill_by_slug(index_data: dict[str, Any], slug: str) -> dict[str, Any] | None:
    for skill in index_data.get("skills") or []:
        if str(skill.get("slug") or "") == slug:
            return skill
    return None


def _require_skill(index_data: dict[str, Any], skill_id: str) -> dict[str, Any]:
    skill = _find_skill(index_data, skill_id)
    if skill is None:
        raise HTTPException(status_code=404, detail="Skill not found")
    return skill


def _require_version(skill: dict[str, Any], version: str) -> dict[str, Any]:
    for item in skill.get("versions") or []:
        if str(item.get("version") or "") == version:
            return item
    raise HTTPException(status_code=404, detail="Skill version not found")


def _normalize_zip_member_path(name: str) -> PurePosixPath | None:
    cleaned = str(name or "").replace("\\", "/").strip("/")
    if not cleaned:
        return None
    path = PurePosixPath(cleaned)
    parts = []
    for part in path.parts:
        if part in ("", "."):
            continue
        if part == "..":
            raise HTTPException(status_code=400, detail="unsafe relative path in zip")
        if ":" in part:
            raise HTTPException(status_code=400, detail="invalid absolute file path in zip")
        parts.append(part)
    if not parts or parts[0] == "__MACOSX" or parts[-1] == ".DS_Store":
        return None
    return PurePosixPath(*parts)


def _strip_single_root(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not entries:
        return entries
    if not all(len(item["path"].parts) > 1 for item in entries):
        return entries
    roots = {item["path"].parts[0] for item in entries}
    if len(roots) != 1:
        return entries
    normalized = []
    for item in entries:
        parts = item["path"].parts[1:]
        normalized.append({**item, "path": PurePosixPath(*parts)})
    return normalized


def _extract_skill_package(zip_bytes: bytes) -> dict[str, Any]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as exc:
        raise HTTPException(status_code=400, detail="invalid zip file") from exc

    entries: list[dict[str, Any]] = []
    for info in archive.infolist():
        if info.is_dir():
            continue
        normalized_path = _normalize_zip_member_path(info.filename)
        if normalized_path is None:
            continue
        entries.append(
            {
                "path": normalized_path,
                "size": int(info.file_size or 0),
                "content": archive.read(info.filename),
            }
        )

    entries = _strip_single_root(entries)
    skill_md_candidates = [item for item in entries if item["path"].name.lower() == "skill.md"]

    if not skill_md_candidates:
        raise HTTPException(status_code=400, detail="zip must contain SKILL.md")
    if len(skill_md_candidates) > 1:
        raise HTTPException(status_code=400, detail="multiple markdown candidates found")

    entry = skill_md_candidates[0]
    try:
        markdown = entry["content"].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="SKILL.md must be UTF-8 encoded") from exc

    references = []
    for item in entries:
        normalized = item["path"].as_posix()
        if normalized == entry["path"].as_posix():
            continue
        references.append({"path": normalized, "size": item["size"]})

    return {
        "entry_file": entry["path"].as_posix(),
        "markdown": markdown,
        "manifest": {"references": references},
        "reference_count": len(references),
    }


def _build_version_record(
    *,
    version: str,
    markdown: str,
    changelog: str,
    entry_file: str,
    manifest: dict[str, Any] | None = None,
    reference_count: int = 0,
) -> dict[str, Any]:
    return {
        "version": str(version or "1.0.0").strip() or "1.0.0",
        "markdown": str(markdown or ""),
        "changelog": str(changelog or ""),
        "entry_file": str(entry_file or "SKILL.md"),
        "manifest": manifest or {"references": []},
        "reference_count": int(reference_count or 0),
        "created_at": _now_iso(),
    }


def _build_skill_record(payload: SkillCreatePayload, version_record: dict[str, Any]) -> dict[str, Any]:
    slug = _normalize_slug(payload.slug or payload.name)
    now = _now_iso()
    return {
        "id": f"skill_{uuid4().hex[:12]}",
        "name": str(payload.name or "").strip(),
        "slug": slug,
        "description": str(payload.description or "").strip(),
        "is_active": True,
        "priority": 50,
        "mode_scope": "skill_assistant",
        "routing_tags": [],
        "trigger_keywords": [],
        "exclude_keywords": [],
        "created_at": now,
        "updated_at": now,
        "deleted_at": None,
        "versions": [version_record],
    }


def _ensure_unique_slug(index_data: dict[str, Any], slug: str) -> None:
    existing = _find_skill_by_slug(index_data, slug)
    if existing and not existing.get("deleted_at"):
        raise HTTPException(status_code=400, detail="slug already exists")


def _resolve_local_zip_path(filename: str) -> Path:
    _ensure_storage()
    candidate = (SKILL_LOCAL_ZIP_ROOT / str(filename or "")).resolve()
    root = SKILL_LOCAL_ZIP_ROOT.resolve()
    if candidate != root and root not in candidate.parents:
        raise HTTPException(status_code=400, detail="invalid local zip path")
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="local zip file not found")
    return candidate


def _list_local_zip_items() -> list[dict[str, Any]]:
    _ensure_storage()
    items = []
    for path in sorted(SKILL_LOCAL_ZIP_ROOT.rglob("*.zip")):
        if not path.is_file():
            continue
        stat = path.stat()
        items.append(
            {
                "filename": path.relative_to(SKILL_LOCAL_ZIP_ROOT).as_posix(),
                "size": int(stat.st_size),
                "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
            }
        )
    return items


def _mode_matches(skill: dict[str, Any], mode: str) -> bool:
    scope = _normalize_mode_scope(skill.get("mode_scope"))
    normalized_mode = _normalize_mode_scope(mode)
    return scope == "both" or scope == normalized_mode


def _resolve_skills(index_data: dict[str, Any], payload: SkillResolvePayload) -> dict[str, Any]:
    text = str(payload.input or "").lower()
    max_count = max(1, min(int(payload.max_count or 3), 10))
    all_skills = [
        skill
        for skill in index_data.get("skills") or []
        if not skill.get("deleted_at") and skill.get("is_active", True) and _mode_matches(skill, payload.mode)
    ]

    manual_ids = {str(item or "") for item in payload.manual_skill_ids if str(item or "").strip()}
    manual_items = []
    for skill in all_skills:
        if str(skill.get("id") or "") in manual_ids:
            manual_items.append(_serialize_skill_summary(skill))

    automatic_scored = []
    for skill in all_skills:
        skill_id = str(skill.get("id") or "")
        if skill_id in manual_ids:
            continue
        exclude_keywords = [item.lower() for item in _normalize_string_list(skill.get("exclude_keywords"))]
        if any(token and token in text for token in exclude_keywords):
            continue

        trigger_keywords = [item.lower() for item in _normalize_string_list(skill.get("trigger_keywords"))]
        routing_tags = [item.lower() for item in _normalize_string_list(skill.get("routing_tags"))]
        match_count = sum(1 for token in trigger_keywords if token and token in text)
        tag_count = sum(1 for token in routing_tags if token and token in text)
        score = int(skill.get("priority", 50) or 50) + match_count * 20 + tag_count * 8
        if trigger_keywords and match_count == 0 and text:
            score -= 25
        automatic_scored.append((score, _serialize_skill_summary(skill)))

    automatic_scored.sort(key=lambda item: (-item[0], item[1]["name"]))
    automatic_items = [item for _, item in automatic_scored[:max_count]]

    combined = manual_items + [item for item in automatic_items if item["id"] not in manual_ids]
    combined = combined[:max_count]

    return {
        "items": combined,
        "manual": manual_items,
        "automatic": automatic_items,
    }


@skill_router.get("/skills/local-zips")
async def list_local_zip_files():
    return {
        "success": True,
        "data": {
            "root_dir": str(SKILL_LOCAL_ZIP_ROOT),
            "items": _list_local_zip_items(),
        },
    }


@skill_router.post("/skills/upload-zip")
async def create_skill_from_zip(
    name: str = Form(...),
    slug: str = Form(""),
    description: str = Form(""),
    version: str = Form("1.0.0"),
    changelog: str = Form(""),
    file: UploadFile = File(...),
):
    raw_bytes = await file.read()
    package = _extract_skill_package(raw_bytes)
    payload = SkillCreatePayload(
        name=name,
        slug=slug,
        description=description,
        version=version,
        markdown=package["markdown"],
        changelog=changelog,
    )

    with _STORE_LOCK:
        index_data = _load_index()
        slug_value = _normalize_slug(payload.slug or payload.name)
        _ensure_unique_slug(index_data, slug_value)
        version_record = _build_version_record(
            version=payload.version,
            markdown=payload.markdown,
            changelog=payload.changelog,
            entry_file=package["entry_file"],
            manifest=package["manifest"],
            reference_count=package["reference_count"],
        )
        skill = _build_skill_record(payload, version_record)
        skill["slug"] = slug_value
        index_data.setdefault("skills", []).append(skill)
        _save_index(index_data)
    return {"success": True, "data": _serialize_skill_summary(skill)}


@skill_router.post("/skills/upload-local-zip")
async def create_skill_from_local_zip(payload: SkillImportLocalZipPayload):
    zip_path = _resolve_local_zip_path(payload.filename)
    package = _extract_skill_package(zip_path.read_bytes())
    create_payload = SkillCreatePayload(
        name=payload.name,
        slug=payload.slug,
        description=payload.description,
        version=payload.version,
        markdown=package["markdown"],
        changelog=payload.changelog,
    )

    with _STORE_LOCK:
        index_data = _load_index()
        slug_value = _normalize_slug(create_payload.slug or create_payload.name)
        _ensure_unique_slug(index_data, slug_value)
        version_record = _build_version_record(
            version=create_payload.version,
            markdown=create_payload.markdown,
            changelog=create_payload.changelog,
            entry_file=package["entry_file"],
            manifest=package["manifest"],
            reference_count=package["reference_count"],
        )
        skill = _build_skill_record(create_payload, version_record)
        skill["slug"] = slug_value
        index_data.setdefault("skills", []).append(skill)
        _save_index(index_data)
    return {"success": True, "data": _serialize_skill_summary(skill)}


@skill_router.post("/skills/resolve/selection")
async def resolve_skill_selection(payload: SkillResolvePayload):
    with _STORE_LOCK:
        index_data = _load_index()
    return {"success": True, "data": _resolve_skills(index_data, payload)}


@skill_router.get("/skills")
async def list_skills(
    page: int = 1,
    page_size: int = 100,
    keyword: str = "",
    is_active: bool | None = None,
    include_deleted: bool = False,
):
    with _STORE_LOCK:
        index_data = _load_index()

    keyword_value = str(keyword or "").strip().lower()
    items = []
    for skill in index_data.get("skills") or []:
        if not include_deleted and skill.get("deleted_at"):
            continue
        if is_active is not None and bool(skill.get("is_active", True)) != is_active:
            continue
        text = " ".join(
            [
                str(skill.get("name") or ""),
                str(skill.get("slug") or ""),
                str(skill.get("description") or ""),
            ]
        ).lower()
        if keyword_value and keyword_value not in text:
            continue
        items.append(_serialize_skill_summary(skill))

    items.sort(key=lambda item: (item.get("updated_at") or "", item.get("name") or ""), reverse=True)
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 100), 200))
    start = (page - 1) * page_size
    end = start + page_size

    return {
        "success": True,
        "data": {
            "items": items[start:end],
            "total": len(items),
            "page": page,
            "page_size": page_size,
        },
    }


@skill_router.post("/skills")
async def create_skill(payload: SkillCreatePayload):
    if not str(payload.name or "").strip():
        raise HTTPException(status_code=400, detail="name is required")
    if not str(payload.markdown or "").strip():
        raise HTTPException(status_code=400, detail="markdown is required")

    with _STORE_LOCK:
        index_data = _load_index()
        slug_value = _normalize_slug(payload.slug or payload.name)
        _ensure_unique_slug(index_data, slug_value)
        version_record = _build_version_record(
            version=payload.version,
            markdown=payload.markdown,
            changelog=payload.changelog,
            entry_file="SKILL.md",
        )
        skill = _build_skill_record(payload, version_record)
        skill["slug"] = slug_value
        index_data.setdefault("skills", []).append(skill)
        _save_index(index_data)
    return {"success": True, "data": _serialize_skill_summary(skill)}


@skill_router.get("/skills/{skill_id}/versions/{version}")
async def get_skill_version(skill_id: str, version: str):
    with _STORE_LOCK:
        index_data = _load_index()
        skill = _require_skill(index_data, skill_id)
        version_record = _require_version(skill, version)
        data = _serialize_skill_version(skill, version_record)
    return {"success": True, "data": data}


@skill_router.patch("/skills/{skill_id}/status")
async def patch_skill_status(skill_id: str, payload: SkillStatusPayload):
    with _STORE_LOCK:
        index_data = _load_index()
        skill = _require_skill(index_data, skill_id)
        skill["is_active"] = bool(payload.is_active)
        skill["updated_at"] = _now_iso()
        _save_index(index_data)
        data = _serialize_skill_summary(skill)
    return {"success": True, "data": data}


@skill_router.patch("/skills/{skill_id}/routing")
async def patch_skill_routing(skill_id: str, payload: SkillRoutingPayload):
    with _STORE_LOCK:
        index_data = _load_index()
        skill = _require_skill(index_data, skill_id)
        if payload.priority is not None:
            skill["priority"] = max(0, min(int(payload.priority), 100))
        if payload.mode_scope is not None:
            skill["mode_scope"] = _normalize_mode_scope(payload.mode_scope)
        if payload.routing_tags is not None:
            skill["routing_tags"] = _normalize_string_list(payload.routing_tags)
        if payload.trigger_keywords is not None:
            skill["trigger_keywords"] = _normalize_string_list(payload.trigger_keywords)
        if payload.exclude_keywords is not None:
            skill["exclude_keywords"] = _normalize_string_list(payload.exclude_keywords)
        skill["updated_at"] = _now_iso()
        _save_index(index_data)
        data = _serialize_skill_summary(skill)
    return {"success": True, "data": data}


@skill_router.get("/skills/{skill_id}")
async def get_skill_detail(skill_id: str):
    with _STORE_LOCK:
        index_data = _load_index()
        skill = _require_skill(index_data, skill_id)
        data = _serialize_skill_detail(skill)
    return {"success": True, "data": data}


@skill_router.patch("/skills/{skill_id}")
async def patch_skill_meta(skill_id: str, payload: SkillMetaPayload):
    with _STORE_LOCK:
        index_data = _load_index()
        skill = _require_skill(index_data, skill_id)
        if payload.name is not None:
            next_name = str(payload.name or "").strip()
            if not next_name:
                raise HTTPException(status_code=400, detail="name cannot be empty")
            skill["name"] = next_name
        if payload.description is not None:
            skill["description"] = str(payload.description or "").strip()
        skill["updated_at"] = _now_iso()
        _save_index(index_data)
        data = _serialize_skill_summary(skill)
    return {"success": True, "data": data}


@skill_router.delete("/skills/{skill_id}")
async def delete_skill(skill_id: str):
    with _STORE_LOCK:
        index_data = _load_index()
        skill = _require_skill(index_data, skill_id)
        skill["is_active"] = False
        skill["deleted_at"] = _now_iso()
        skill["updated_at"] = skill["deleted_at"]
        _save_index(index_data)
        data = _serialize_skill_summary(skill)
    return {"success": True, "data": data}
