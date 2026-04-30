import asyncio
import json
import logging
import mimetypes
import os
import time
from copy import deepcopy
from html import escape
from io import BytesIO
from typing import Dict, List, Optional
from urllib.parse import urlparse

import httpx
from openai import AsyncOpenAI
from PIL import Image, UnidentifiedImageError

from db import (
    create_wechat_publish_record,
    get_wechat_publish_record,
    get_wechat_publish_record_by_publish_id,
    update_wechat_publish_record,
)
from env_loader import get_dashscope_api_key, load_crawl_env


load_crawl_env()

logger = logging.getLogger("wechat_mp_service")

WECHAT_API_BASE = "https://api.weixin.qq.com"
WECHAT_MP_ENABLE = str(os.getenv("WECHAT_MP_ENABLE", "0")).strip().lower() not in {
    "",
    "0",
    "false",
    "off",
    "no",
}
WECHAT_MP_APP_ID = str(os.getenv("WECHAT_MP_APP_ID", "") or "").strip()
WECHAT_MP_APP_SECRET = str(os.getenv("WECHAT_MP_APP_SECRET", "") or "").strip()
WECHAT_MP_AUTHOR = str(os.getenv("WECHAT_MP_AUTHOR", "AI趣闻萃取") or "").strip() or "AI趣闻萃取"
WECHAT_MP_TIMEOUT = float(str(os.getenv("WECHAT_MP_TIMEOUT", "20") or "20").strip())
WECHAT_MP_OPEN_COMMENT = int(str(os.getenv("WECHAT_MP_OPEN_COMMENT", "0") or "0").strip() or "0")
WECHAT_MP_FANS_COMMENT_ONLY = int(
    str(os.getenv("WECHAT_MP_FANS_COMMENT_ONLY", "0") or "0").strip() or "0"
)
WECHAT_MP_DIGEST_MAX_BYTES = int(str(os.getenv("WECHAT_MP_DIGEST_MAX_BYTES", "120") or "120").strip() or "120")
WECHAT_REWRITE_ENABLED = str(os.getenv("WECHAT_REWRITE_ENABLED", "1") or "1").strip().lower() not in {
    "",
    "0",
    "false",
    "off",
    "no",
}
WECHAT_REWRITE_MODEL = str(os.getenv("WECHAT_REWRITE_MODEL", "qwen-plus") or "qwen-plus").strip() or "qwen-plus"
WECHAT_REWRITE_TEMPERATURE = float(str(os.getenv("WECHAT_REWRITE_TEMPERATURE", "0.7") or "0.7").strip() or "0.7")
WECHAT_REWRITE_PROMPT_VERSION = "wechat-brief-v2"
WECHAT_MP_DRAFT_ONLY = str(os.getenv("WECHAT_MP_DRAFT_ONLY", "0") or "0").strip().lower() not in {
    "",
    "0",
    "false",
    "off",
    "no",
}
WECHAT_MP_APPEND_ORIGINAL_LINK = str(
    os.getenv("WECHAT_MP_APPEND_ORIGINAL_LINK", "1") or "1"
).strip().lower() not in {
    "0",
    "false",
    "off",
    "no",
}
WECHAT_MP_DEFAULT_COVER_SOURCES = [
    str(os.getenv("WECHAT_MP_DEFAULT_COVER_1", "") or "").strip(),
    str(os.getenv("WECHAT_MP_DEFAULT_COVER_2", "") or "").strip(),
    str(os.getenv("WECHAT_MP_DEFAULT_COVER_3", "") or "").strip(),
]
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_WECHAT_COVER_DIR = os.path.join(MODULE_DIR, "static", "wechat-covers")
BUILTIN_WECHAT_COVER_SOURCES = [
    os.path.join(DEFAULT_WECHAT_COVER_DIR, "ai-fallback-cover-1.png"),
    os.path.join(DEFAULT_WECHAT_COVER_DIR, "ai-fallback-cover-2.png"),
    os.path.join(DEFAULT_WECHAT_COVER_DIR, "ai-fallback-cover-3.png"),
]
DASHSCOPE_API_KEY = get_dashscope_api_key()
DASHSCOPE_BASE_URL = (
    str(os.getenv("DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1") or "").strip()
    or "https://dashscope.aliyuncs.com/compatible-mode/v1"
)

INVALID_TOKEN_ERRCODES = {40001, 40014, 42001}
PUBLISH_STATUS_MAP = {
    0: "published",
    1: "publishing",
    2: "original_failed",
    3: "failed",
    4: "platform_rejected",
    5: "deleted",
    6: "banned",
}
PUBLISH_STATUS_LABELS = {
    0: "published",
    1: "publishing",
    2: "original failed",
    3: "failed",
    4: "platform rejected",
    5: "deleted after publish",
    6: "banned after publish",
}
SUPPORTED_IMAGE_TYPES = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
}

_token_cache = {"access_token": "", "expires_at": 0.0}
_token_lock = asyncio.Lock()
_cover_media_cache: Dict[str, str] = {}
_rewrite_llm_client: Optional[AsyncOpenAI] = None


def is_wechat_mp_configured() -> bool:
    return WECHAT_MP_ENABLE and bool(WECHAT_MP_APP_ID) and bool(WECHAT_MP_APP_SECRET)


def get_wechat_mp_config_status() -> Dict:
    return {
        "enabled": WECHAT_MP_ENABLE,
        "configured": is_wechat_mp_configured(),
        "hasAppId": bool(WECHAT_MP_APP_ID),
        "hasAppSecret": bool(WECHAT_MP_APP_SECRET),
        "author": WECHAT_MP_AUTHOR,
        "draftOnly": WECHAT_MP_DRAFT_ONLY,
        "rewriteEnabled": WECHAT_REWRITE_ENABLED,
        "hasDashscopeKey": bool(DASHSCOPE_API_KEY),
    }


def _timeout() -> httpx.Timeout:
    return httpx.Timeout(WECHAT_MP_TIMEOUT, connect=min(10.0, WECHAT_MP_TIMEOUT))


def _clip_text(value: str, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _clip_utf8_bytes(value: str, max_bytes: int, suffix: str = "...") -> str:
    text = str(value or "").strip()
    if max_bytes <= 0:
        return ""
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text

    suffix_bytes = str(suffix or "").encode("utf-8")
    if len(suffix_bytes) >= max_bytes:
        return encoded[:max_bytes].decode("utf-8", errors="ignore")

    budget = max_bytes - len(suffix_bytes)
    clipped = encoded[:budget].decode("utf-8", errors="ignore").rstrip()
    return f"{clipped}{suffix}"


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _normalize_text_list(value) -> List[str]:
    if isinstance(value, list):
        return _dedupe_preserve_order([str(item or "").strip() for item in value if str(item or "").strip()])
    text = str(value or "").strip()
    return [text] if text else []


def _build_fallback_question_headline(headline: str, limit: int = 32) -> str:
    text = str(headline or "").strip()
    if not text:
        return "今天这条 AI 消息，到底在说啥？"
    if len(text) <= 18 and not any(mark in text for mark in ("？", "?", "！", "!")):
        return _clip_text(f"{text}，到底在说啥？", limit)
    return _clip_text(text, limit)


def _condense_fact_text(value: str, limit: int = 80) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("\r", " ").replace("\n", " ")
    for sep in ("。", "！", "？", "；", ";"):
        if sep in text:
            first = text.split(sep, 1)[0].strip()
            if first:
                text = first
                break
    return _clip_text(text, limit)


def _extract_json_object(text: str) -> Dict:
    raw = str(text or "").strip()
    if not raw:
        return {}

    if raw.startswith("```"):
        segments = raw.split("```")
        for segment in segments:
            candidate = segment.strip()
            if not candidate:
                continue
            if candidate.startswith("json"):
                candidate = candidate[4:].strip()
            if candidate.startswith("{") and candidate.endswith("}"):
                raw = candidate
                break

    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _can_use_wechat_rewrite_agent() -> bool:
    return WECHAT_REWRITE_ENABLED and bool(DASHSCOPE_API_KEY)


def _get_wechat_rewrite_llm_client() -> AsyncOpenAI:
    global _rewrite_llm_client
    if _rewrite_llm_client is None:
        _rewrite_llm_client = AsyncOpenAI(
            api_key=DASHSCOPE_API_KEY,
            base_url=DASHSCOPE_BASE_URL,
        )
    return _rewrite_llm_client


def _build_wechat_rewrite_prompt(issue: Dict) -> str:
    materials = []
    for index, item in enumerate(issue.get("items") or [], 1):
        brief = item.get("brief") if isinstance(item.get("brief"), dict) else {}
        materials.append(
            {
                "sortIndex": index,
                "headline": str(item.get("headline") or "").strip(),
                "source": str(item.get("source") or "").strip(),
                "warning": str(item.get("warning") or "").strip(),
                "paragraphs": _normalize_text_list(item.get("expandedBody"))[:4],
                "tags": _normalize_text_list(brief.get("tags"))[:4],
                "articleUrl": str(item.get("articleUrl") or "").strip(),
            }
        )

    schema = {
        "items": [
            {
                "sortIndex": "整数，必须与输入一致",
                "headline": "更好懂、更有趣的标题，14-28字，允许轻微悬念但不能夸张",
                "warning": "一句开场白，像和第一次接触 AI 的读者聊天，28-60字",
                "expandedBody": ["5-7段短文，每段1-2句，口语化、像讲故事、幼儿园也能读懂"],
                "tags": ["2-4个短标签"],
            }
        ]
    }

    return (
        "你是公众号栏目《AI趣闻萃取》的改写编辑。\n"
        "请把输入的新闻改写成更好懂、更有趣、更像讲故事的公众号版本。\n"
        "必须遵守这些规则：\n"
        "1. 绝不编造事实，不添加输入里没有的人名、数字、日期、结论。\n"
        "2. 语言要像给完全不懂 AI 的小白解释，幼儿园都能大致看懂。\n"
        "3. 先把术语翻成人话，再讲新闻本身；可以用生活化比喻，但比喻不能改变事实。\n"
        "4. 每条正文 expandedBody 写 5-7 段，每段 1-2 句，读起来轻松，但整体要比输入更完整。\n"
        "5. 标题可以更抓眼球，但不能标题党。\n"
        "6. warning 要像文章开头第一句，把读者拉进来。\n"
        "7. 正文结构尽量做到：先讲这事在说什么，再讲原文提到的关键信息，再讲为什么普通人值得看，最后提醒去看原文确认细节。\n"
        "8. 只输出 JSON，不要输出解释，不要 Markdown。\n\n"
        f"返回 JSON Schema：{json.dumps(schema, ensure_ascii=False)}\n\n"
        f"输入素材：{json.dumps(materials, ensure_ascii=False)}"
    )


def _fallback_expand_item_for_wechat(item: Dict) -> Dict:
    original_headline = str(item.get("headline") or "").strip()
    original_warning = str(item.get("warning") or "").strip()
    original_body = _normalize_text_list(item.get("expandedBody"))
    brief = item.get("brief") if isinstance(item.get("brief"), dict) else {}
    tags = _normalize_text_list(brief.get("tags"))[:4] or ["AI趣闻", "今日热榜"]
    source = str(item.get("source") or "").strip()

    facts = _dedupe_preserve_order(
        [
            _condense_fact_text(original_warning, 84),
            *[_condense_fact_text(paragraph, 84) for paragraph in original_body[:3]],
            _condense_fact_text(str(brief.get("lead") or "").strip(), 84),
            _condense_fact_text(original_headline, 48),
        ]
    )
    main_fact = facts[0] if facts else _condense_fact_text(original_headline, 48)
    extra_fact = facts[1] if len(facts) > 1 else ""
    reminder = _condense_fact_text(original_warning or main_fact or original_headline, 60)
    source_hint = f"这条内容来自 {source}，所以更适合把它当成一个值得继续观察的信号。" if source else ""

    paragraphs = _dedupe_preserve_order(
        [
            f"如果把这条消息翻成人话，它现在最想告诉你的其实是：{main_fact}",
            f"别急着被术语吓到，你可以先把它理解成 AI 圈又出现了一个值得多看一眼的新动作。",
            f"原文里更具体的一点是：{extra_fact}" if extra_fact else "",
            "对普通读者来说，你现在不用背概念，先知道“有件新变化正在发生”就够了。",
            source_hint,
            f"如果你只想先记住一句话，那就是：{reminder}",
            "想把它看得更准一点，最稳的办法还是点开原文，把完整上下文看一遍。",
        ]
    )
    paragraphs = paragraphs[:6]
    return {
        **item,
        "headline": _build_fallback_question_headline(original_headline, 32),
        "warning": _clip_text(
            _condense_fact_text(original_warning, 84)
            or f"先别被专业词吓到，这条新闻其实在说：{main_fact or _condense_fact_text(original_headline, 48)}",
            90,
        ),
        "expandedBody": paragraphs,
        "brief": {
            **brief,
            "headline": _build_fallback_question_headline(original_headline, 32),
            "lead": _clip_text(
                _condense_fact_text(original_warning, 84)
                or f"先别被专业词吓到，这条新闻其实在说：{main_fact or _condense_fact_text(original_headline, 48)}",
                90,
            ),
            "paragraphs": paragraphs,
            "tags": tags,
            "rewriteApplied": False,
            "rewriteMode": "wechat_fallback",
        },
    }


def _merge_wechat_rewrite_item(base_item: Dict, rewrite_item: Optional[Dict]) -> Dict:
    if not isinstance(rewrite_item, dict):
        return _fallback_expand_item_for_wechat(base_item)

    base_brief = base_item.get("brief") if isinstance(base_item.get("brief"), dict) else {}
    headline = _clip_text(str(rewrite_item.get("headline") or base_item.get("headline") or "").strip(), 32)
    warning = _clip_text(str(rewrite_item.get("warning") or base_item.get("warning") or headline).strip(), 90)
    body = _normalize_text_list(rewrite_item.get("expandedBody")) or _normalize_text_list(base_item.get("expandedBody"))
    if len(body) < 4:
        return _fallback_expand_item_for_wechat(base_item)
    tags = _normalize_text_list(rewrite_item.get("tags"))[:4] or _normalize_text_list(base_brief.get("tags"))[:4] or ["AI趣闻", "今日热榜"]
    return {
        **base_item,
        "headline": headline,
        "warning": warning,
        "expandedBody": body,
        "brief": {
            **base_brief,
            "headline": headline,
            "lead": warning,
            "paragraphs": body,
            "tags": tags,
            "rewriteApplied": True,
            "rewriteMode": "wechat_agent",
            "rewriteModel": WECHAT_REWRITE_MODEL,
            "rewritePromptVersion": WECHAT_REWRITE_PROMPT_VERSION,
        },
    }


async def _rewrite_issue_for_wechat(issue: Dict) -> Dict:
    base_issue = deepcopy(issue if isinstance(issue, dict) else {})
    items = base_issue.get("items") if isinstance(base_issue.get("items"), list) else []
    if not items:
        return base_issue

    if not _can_use_wechat_rewrite_agent():
        base_issue["items"] = [_fallback_expand_item_for_wechat(item) for item in items]
        base_issue["wechatRewriteApplied"] = False
        base_issue["wechatRewriteMode"] = "fallback"
        return base_issue

    prompt = _build_wechat_rewrite_prompt(base_issue)
    try:
        response = await _get_wechat_rewrite_llm_client().chat.completions.create(
            model=WECHAT_REWRITE_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=WECHAT_REWRITE_TEMPERATURE,
        )
        content = str(response.choices[0].message.content or "").strip()
        payload = _extract_json_object(content)
    except Exception as exc:
        logger.warning("wechat rewrite failed: %s", exc)
        base_issue["items"] = [_fallback_expand_item_for_wechat(item) for item in items]
        base_issue["wechatRewriteApplied"] = False
        base_issue["wechatRewriteMode"] = f"fallback:{exc}"
        return base_issue

    payload_items = payload.get("items")
    if not isinstance(payload_items, list):
        base_issue["items"] = [_fallback_expand_item_for_wechat(item) for item in items]
        base_issue["wechatRewriteApplied"] = False
        base_issue["wechatRewriteMode"] = "fallback:invalid_payload"
        return base_issue

    rewrite_by_index = {}
    for raw_item in payload_items:
        if not isinstance(raw_item, dict):
            continue
        try:
            index = int(raw_item.get("sortIndex") or 0)
        except (TypeError, ValueError):
            continue
        if index > 0:
            rewrite_by_index[index] = raw_item

    merged_items = []
    rewritten_count = 0
    for index, item in enumerate(items, 1):
        merged = _merge_wechat_rewrite_item(item, rewrite_by_index.get(index))
        if merged.get("brief", {}).get("rewriteMode") == "wechat_agent":
            rewritten_count += 1
        merged_items.append(merged)

    base_issue["items"] = merged_items
    base_issue["wechatRewriteApplied"] = rewritten_count > 0
    base_issue["wechatRewriteMode"] = "agent" if rewritten_count > 0 else "fallback:empty"
    return base_issue


def _guess_filename(url: str, content_type: str) -> str:
    parsed = urlparse(str(url or "").strip())
    basename = os.path.basename(parsed.path or "").strip() or "cover"
    _, ext = os.path.splitext(basename)
    if ext:
        return basename
    guessed = SUPPORTED_IMAGE_TYPES.get(content_type or "") or mimetypes.guess_extension(content_type or "") or ".jpg"
    return f"{basename}{guessed}"


def _normalize_image_bytes(image_bytes: bytes, content_type: str, filename: str) -> tuple[bytes, str, str]:
    normalized_type = str(content_type or "").split(";", 1)[0].strip().lower()
    normalized_name = filename or "cover.jpg"

    if normalized_type in SUPPORTED_IMAGE_TYPES:
        return image_bytes, normalized_type, normalized_name

    try:
        image = Image.open(BytesIO(image_bytes))
        if image.mode not in ("RGB", "L"):
            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.split()[-1] if image.mode in ("RGBA", "LA") else None)
            image = background
        elif image.mode == "L":
            image = image.convert("RGB")

        output = BytesIO()
        image.save(output, format="JPEG", quality=92, optimize=True)
        return output.getvalue(), "image/jpeg", os.path.splitext(normalized_name)[0] + ".jpg"
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise RuntimeError(f"unsupported cover image format: {exc}") from exc


def _resolve_local_image_path(source: str) -> str:
    normalized = str(source or "").strip()
    if not normalized:
        return ""

    parsed = urlparse(normalized)
    if parsed.scheme in {"http", "https"}:
        return ""

    if parsed.scheme == "file":
        candidate = parsed.path or ""
        if os.name == "nt" and candidate.startswith("/"):
            candidate = candidate.lstrip("/")
        return os.path.abspath(candidate)

    if parsed.scheme and len(parsed.scheme) > 1:
        return ""

    direct_candidate = os.path.abspath(normalized)
    if os.path.exists(direct_candidate):
        return direct_candidate

    return os.path.abspath(os.path.join(MODULE_DIR, normalized))


def _default_cover_sources() -> List[str]:
    sources: List[str] = []

    for idx, source in enumerate(WECHAT_MP_DEFAULT_COVER_SOURCES):
        candidate = source or BUILTIN_WECHAT_COVER_SOURCES[idx]
        local_path = _resolve_local_image_path(candidate)
        if local_path:
            if os.path.exists(local_path):
                sources.append(local_path)
            else:
                logger.warning("wechat default cover missing: %s", local_path)
            continue

        normalized_candidate = str(candidate or "").strip()
        if normalized_candidate:
            sources.append(normalized_candidate)

    return sources


async def _get_access_token(force_refresh: bool = False) -> str:
    if not WECHAT_MP_APP_ID or not WECHAT_MP_APP_SECRET:
        raise RuntimeError("wechat mp credentials are missing")

    now = time.time()
    if not force_refresh and _token_cache["access_token"] and _token_cache["expires_at"] > now + 60:
        return str(_token_cache["access_token"])

    async with _token_lock:
        now = time.time()
        if not force_refresh and _token_cache["access_token"] and _token_cache["expires_at"] > now + 60:
            return str(_token_cache["access_token"])

        async with httpx.AsyncClient(timeout=_timeout()) as client:
            response = await client.get(
                f"{WECHAT_API_BASE}/cgi-bin/token",
                params={
                    "grant_type": "client_credential",
                    "appid": WECHAT_MP_APP_ID,
                    "secret": WECHAT_MP_APP_SECRET,
                },
            )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("wechat token api returned invalid JSON") from exc

        errcode = int(payload.get("errcode") or 0)
        if errcode:
            raise RuntimeError(
                f"wechat token api error {errcode}: {payload.get('errmsg') or 'unknown error'}"
            )

        access_token = str(payload.get("access_token") or "").strip()
        expires_in = int(payload.get("expires_in") or 7200)
        if not access_token:
            raise RuntimeError("wechat token api returned empty access_token")

        _token_cache["access_token"] = access_token
        _token_cache["expires_at"] = time.time() + max(300, expires_in - 120)
        return access_token


async def _wechat_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict] = None,
    json_payload: Optional[Dict] = None,
    data: Optional[Dict] = None,
    files=None,
    retry_on_invalid_token: bool = True,
) -> Dict:
    query = dict(params or {})
    query["access_token"] = await _get_access_token(force_refresh=False)

    async with httpx.AsyncClient(timeout=_timeout()) as client:
        response = await client.request(
            method=method,
            url=f"{WECHAT_API_BASE}{path}",
            params=query,
            json=json_payload,
            data=data,
            files=files,
        )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"wechat api returned invalid JSON for {path}") from exc

    errcode = int(payload.get("errcode") or 0)
    if errcode in INVALID_TOKEN_ERRCODES and retry_on_invalid_token:
        _token_cache["access_token"] = ""
        _token_cache["expires_at"] = 0.0
        return await _wechat_request(
            method,
            path,
            params=params,
            json_payload=json_payload,
            data=data,
            files=files,
            retry_on_invalid_token=False,
        )
    if errcode:
        raise RuntimeError(f"wechat api error {errcode}: {payload.get('errmsg') or 'unknown error'}")
    return payload if isinstance(payload, dict) else {}


async def _download_cover_image(cover_url: str) -> tuple[bytes, str, str]:
    local_path = _resolve_local_image_path(cover_url)
    if local_path:
        if not os.path.exists(local_path):
            raise RuntimeError(f"cover image file does not exist: {local_path}")
        with open(local_path, "rb") as file_obj:
            raw_bytes = file_obj.read()
        content_type = str(mimetypes.guess_type(local_path)[0] or "image/png").strip().lower()
        filename = os.path.basename(local_path)
        image_bytes, normalized_type, normalized_name = _normalize_image_bytes(
            raw_bytes,
            content_type,
            filename,
        )
        if len(image_bytes) > 10 * 1024 * 1024:
            raise RuntimeError("cover image exceeds WeChat 10MB limit")
        return image_bytes, normalized_type, normalized_name

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
        )
    }
    async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True, headers=headers) as client:
        response = await client.get(cover_url)
    response.raise_for_status()
    content_type = str(response.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    filename = _guess_filename(cover_url, content_type)
    image_bytes, normalized_type, normalized_name = _normalize_image_bytes(
        response.content,
        content_type,
        filename,
    )
    if len(image_bytes) > 10 * 1024 * 1024:
        raise RuntimeError("cover image exceeds WeChat 10MB limit")
    return image_bytes, normalized_type, normalized_name


async def _upload_cover_image(cover_url: str) -> str:
    normalized_url = str(cover_url or "").strip()
    if not normalized_url:
        raise RuntimeError("cover image is missing")
    if normalized_url in _cover_media_cache:
        return _cover_media_cache[normalized_url]

    image_bytes, content_type, filename = await _download_cover_image(normalized_url)
    payload = await _wechat_request(
        "POST",
        "/cgi-bin/material/add_material",
        params={"type": "image"},
        files={"media": (filename, image_bytes, content_type)},
    )
    media_id = str(payload.get("media_id") or "").strip()
    if not media_id:
        raise RuntimeError("wechat material api returned empty media_id")
    _cover_media_cache[normalized_url] = media_id
    return media_id


def _article_paragraphs(item: Dict) -> List[str]:
    paragraphs = item.get("expandedBody")
    if isinstance(paragraphs, list):
        normalized = _dedupe_preserve_order([str(value or "").strip() for value in paragraphs if str(value or "").strip()])
        if normalized:
            return normalized
    brief = item.get("brief") if isinstance(item.get("brief"), dict) else {}
    raw = brief.get("paragraphs")
    if isinstance(raw, list):
        normalized = _dedupe_preserve_order([str(value or "").strip() for value in raw if str(value or "").strip()])
        if normalized:
            return normalized
    warning = str(item.get("warning") or "").strip()
    return [warning] if warning else []


def _render_article_html(issue: Dict, item: Dict) -> str:
    headline = str(item.get("headline") or "").strip()
    warning = str(item.get("warning") or "").strip()
    source = str(item.get("source") or "").strip()
    article_url = str(item.get("articleUrl") or "").strip()
    paragraphs = [paragraph for paragraph in _article_paragraphs(item) if paragraph != warning]
    brief = item.get("brief") if isinstance(item.get("brief"), dict) else {}
    tags = brief.get("tags") if isinstance(brief.get("tags"), list) else []
    normalized_tags = [str(tag or "").strip() for tag in tags if str(tag or "").strip()]

    parts = [
        '<section style="font-size:16px;line-height:1.8;color:#1f2937;">',
    ]
    if warning:
        parts.append(
            f'<p style="margin:0 0 16px;font-weight:700;color:#0f172a;">{escape(warning)}</p>'
        )
    for paragraph in paragraphs:
        parts.append(f'<p style="margin:0 0 14px;">{escape(paragraph)}</p>')
    if normalized_tags:
        parts.append(
            '<p style="margin:18px 0 0;color:#64748b;font-size:13px;">'
            f'关键词：{escape(" / ".join(normalized_tags[:5]))}'
            "</p>"
        )
    meta_bits = [bit for bit in [issue.get("date"), source] if str(bit or "").strip()]
    if meta_bits:
        parts.append(
            '<p style="margin:8px 0 0;color:#64748b;font-size:13px;">'
            f'{escape(" · ".join(str(bit).strip() for bit in meta_bits))}'
            "</p>"
        )
    if WECHAT_MP_APPEND_ORIGINAL_LINK and article_url:
        safe_url = escape(article_url, quote=True)
        link_text = escape(f"查看原文：{headline or '原文链接'}")
        parts.append(
            '<p style="margin:18px 0 0;">'
            f'<a href="{safe_url}" style="color:#2563eb;text-decoration:none;">{link_text}</a>'
            "</p>"
        )
    parts.append("</section>")
    return "".join(parts)


def _first_available_cover(issue: Dict) -> str:
    for item in issue.get("items") or []:
        cover = str(item.get("coverImage") or "").strip()
        if cover:
            return cover
    return ""


async def _build_wechat_articles(issue: Dict) -> List[Dict]:
    items = issue.get("items") if isinstance(issue.get("items"), list) else []
    if not items:
        raise RuntimeError("issue has no items to publish")

    fallback_cover = _first_available_cover(issue)
    default_covers = _default_cover_sources()
    if not fallback_cover and not default_covers:
        raise RuntimeError("no cover image available for the selected issue")

    articles = []
    for index, item in enumerate(items):
        cover_url = str(item.get("coverImage") or "").strip() or fallback_cover
        if not cover_url and default_covers:
            cover_url = default_covers[index % len(default_covers)]
        thumb_media_id = await _upload_cover_image(cover_url)
        title = _clip_text(str(item.get("headline") or "").strip(), 64) or "AI趣闻萃取"
        digest_source = str(item.get("warning") or "").strip()
        if not digest_source:
            paragraphs = _article_paragraphs(item)
            digest_source = paragraphs[0] if paragraphs else title
        digest = _clip_utf8_bytes(digest_source, WECHAT_MP_DIGEST_MAX_BYTES)
        article_url = str(item.get("articleUrl") or "").strip()
        articles.append(
            {
                "article_type": "news",
                "title": title,
                "author": WECHAT_MP_AUTHOR,
                "digest": digest,
                "content": _render_article_html(issue, item),
                "content_source_url": article_url,
                "thumb_media_id": thumb_media_id,
                "need_open_comment": WECHAT_MP_OPEN_COMMENT,
                "only_fans_can_comment": WECHAT_MP_FANS_COMMENT_ONLY,
            }
        )
    return articles


def _normalize_fail_idx(value) -> List[int]:
    if isinstance(value, list):
        result = []
        for item in value:
            try:
                result.append(int(item))
            except (TypeError, ValueError):
                continue
        return result
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return []
    return [normalized]


def _normalize_publish_result(payload: Dict) -> Dict:
    publish_status = int(payload.get("publish_status") or -1)
    article_urls = []
    article_detail = payload.get("article_detail") if isinstance(payload.get("article_detail"), dict) else {}
    detail_items = article_detail.get("item") if isinstance(article_detail.get("item"), list) else []
    for item in detail_items:
        if not isinstance(item, dict):
            continue
        url = str(item.get("article_url") or "").strip()
        if url:
            article_urls.append(url)

    return {
        "publishId": str(payload.get("publish_id") or "").strip(),
        "publishStatus": publish_status,
        "status": PUBLISH_STATUS_MAP.get(publish_status, "unknown"),
        "statusLabel": PUBLISH_STATUS_LABELS.get(publish_status, "unknown"),
        "articleId": str(payload.get("article_id") or "").strip(),
        "articleUrls": article_urls,
        "failIdx": _normalize_fail_idx(payload.get("fail_idx")),
        "raw": payload if isinstance(payload, dict) else {},
    }


async def _create_single_article_draft(article: Dict) -> Dict:
    draft_payload = await _wechat_request(
        "POST",
        "/cgi-bin/draft/add",
        json_payload={"articles": [article]},
    )
    draft_media_id = str(draft_payload.get("media_id") or "").strip()
    if not draft_media_id:
        raise RuntimeError("wechat draft api returned empty media_id")
    return {
        "draftMediaId": draft_media_id,
        "raw": draft_payload if isinstance(draft_payload, dict) else {},
    }


async def publish_issue_to_wechat(issue: Dict, *, created_by: str = "") -> Dict:
    if not is_wechat_mp_configured():
        raise RuntimeError("wechat mp publish is not configured")

    issue_id = str(issue.get("id") or "").strip()
    if not issue_id:
        raise RuntimeError("issue id is missing")

    record = await create_wechat_publish_record(
        issue_id,
        created_by=created_by,
        status="preparing",
    )
    record_id = int(record.get("id") or 0)

    try:
        issue_for_wechat = await _rewrite_issue_for_wechat(issue)
        articles = await _build_wechat_articles(issue_for_wechat)
        if WECHAT_MP_DRAFT_ONLY:
            draft_results = []
            for index, article in enumerate(articles, start=1):
                single_draft = await _create_single_article_draft(article)
                draft_results.append(
                    {
                        "sortIndex": index,
                        "title": str(article.get("title") or "").strip(),
                        "draftMediaId": single_draft["draftMediaId"],
                        "raw": single_draft["raw"],
                    }
                )

            await update_wechat_publish_record(
                record_id,
                draft_media_id=",".join(item["draftMediaId"] for item in draft_results if item.get("draftMediaId")),
                status="draft_created",
                raw={
                    "drafts": draft_results,
                    "wechatRewriteApplied": bool(issue_for_wechat.get("wechatRewriteApplied")),
                    "wechatRewriteMode": str(issue_for_wechat.get("wechatRewriteMode") or "").strip(),
                },
            )
            record = await get_wechat_publish_record(record_id)
            return {
                "issueId": issue_id,
                "draftMediaId": draft_results[0]["draftMediaId"] if draft_results else "",
                "draftMediaIds": [item["draftMediaId"] for item in draft_results if item.get("draftMediaId")],
                "draftCount": len(draft_results),
                "drafts": draft_results,
                "publishId": "",
                "draftOnly": True,
                "status": {
                    "status": "draft_created",
                    "statusLabel": "drafts created",
                    "publishStatus": -1,
                    "articleUrls": [],
                    "failIdx": [],
                    "raw": {
                        "drafts": draft_results,
                        "wechatRewriteApplied": bool(issue_for_wechat.get("wechatRewriteApplied")),
                        "wechatRewriteMode": str(issue_for_wechat.get("wechatRewriteMode") or "").strip(),
                    },
                },
                "record": record,
            }

        draft_payload = await _wechat_request(
            "POST",
            "/cgi-bin/draft/add",
            json_payload={"articles": articles},
        )
        draft_media_id = str(draft_payload.get("media_id") or "").strip()
        if not draft_media_id:
            raise RuntimeError("wechat draft api returned empty media_id")

        await update_wechat_publish_record(
            record_id,
            draft_media_id=draft_media_id,
            status="draft_created",
            raw={"draft": draft_payload},
        )

        submit_payload = await _wechat_request(
            "POST",
            "/cgi-bin/freepublish/submit",
            json_payload={"media_id": draft_media_id},
        )
        publish_id = str(submit_payload.get("publish_id") or "").strip()
        if not publish_id:
            raise RuntimeError("wechat publish api returned empty publish_id")

        await update_wechat_publish_record(
            record_id,
            publish_id=publish_id,
            publish_status=1,
            status="publishing",
            raw={"draft": draft_payload, "submit": submit_payload},
        )
        synced = await sync_wechat_publish_status(publish_id, record_id=record_id)
        latest = synced.get("record") if isinstance(synced, dict) else None
        return {
            "issueId": issue_id,
            "draftMediaId": draft_media_id,
            "publishId": publish_id,
            "status": synced.get("status") if isinstance(synced, dict) else {},
            "record": latest or await get_wechat_publish_record(record_id),
        }
    except Exception as exc:
        logger.exception("wechat publish failed issue_id=%s", issue_id)
        await update_wechat_publish_record(
            record_id,
            status="failed",
            error_message=str(exc),
        )
        raise


async def sync_wechat_publish_status(publish_id: str, *, record_id: int = 0) -> Dict:
    normalized_publish_id = str(publish_id or "").strip()
    if not normalized_publish_id:
        raise RuntimeError("publish_id is required")

    payload = await _wechat_request(
        "POST",
        "/cgi-bin/freepublish/get",
        json_payload={"publish_id": normalized_publish_id},
    )
    normalized = _normalize_publish_result(payload)

    target_record = None
    if record_id > 0:
        target_record = await update_wechat_publish_record(
            record_id,
            publish_id=normalized["publishId"],
            publish_status=normalized["publishStatus"],
            status=normalized["status"],
            article_id=normalized["articleId"],
            article_urls=normalized["articleUrls"],
            fail_idx=normalized["failIdx"],
            raw=normalized["raw"],
            error_message="",
        )
    else:
        existing = await get_wechat_publish_record_by_publish_id(normalized_publish_id)
        if existing:
            target_record = await update_wechat_publish_record(
                int(existing.get("id") or 0),
                publish_status=normalized["publishStatus"],
                status=normalized["status"],
                article_id=normalized["articleId"],
                article_urls=normalized["articleUrls"],
                fail_idx=normalized["failIdx"],
                raw=normalized["raw"],
                error_message="",
            )
        else:
            target_record = existing

    return {
        "publishId": normalized["publishId"],
        "status": normalized,
        "record": target_record,
    }
