import json
import logging
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import httpx
from openai import AsyncOpenAI

from db import (
    get_articles_by_ids,
    get_top_articles,
    list_push_devices,
    save_push_brief_issue,
)
from env_loader import get_dashscope_api_key, load_crawl_env


load_crawl_env()

logger = logging.getLogger("push_service")
INVALID_PUSH_CLIENT_IDS = {"null", "undefined", "nil", "none", "cid_not_support"}

UNIPUSH_HTTP_ENDPOINT = os.getenv("UNIPUSH_HTTP_ENDPOINT", "").strip()
UNIPUSH_HTTP_AUTH_TOKEN = os.getenv("UNIPUSH_HTTP_AUTH_TOKEN", "").strip()
UNIPUSH_HTTP_TIMEOUT = float(os.getenv("UNIPUSH_HTTP_TIMEOUT", "20").strip() or "20")

DEFAULT_PUSH_ROUTE = (
    os.getenv("UNIPUSH_DEFAULT_ROUTE", "/pages/news-brief/issue").strip()
    or "/pages/news-brief/issue"
)
DEFAULT_PUSH_TITLE = os.getenv("UNIPUSH_DAILY_TITLE", "今日 AI 资讯已更新").strip() or "今日 AI 资讯已更新"
DEFAULT_PUSH_BODY = (
    os.getenv("UNIPUSH_DAILY_BODY", "你关注的 AI 热点已更新，点击查看今天的精选资讯").strip()
    or "你关注的 AI 热点已更新，点击查看今天的精选资讯"
)
DEFAULT_SELECTED_PUSH_TITLE = "AI趣闻萃取已更新"
DEFAULT_SELECTED_PUSH_BODY = "为你挑了今天最值得点开的 3 条 AI 新闻"

DEFAULT_ISSUE_SUBTITLE = (
    "这里有为你精心挑选的 AI 智慧与生活点滴，每天 7 点准时更新，希望能伴你度过轻松且有收获的每一天。"
)
DEFAULT_ISSUE_FOOTER = "AI趣闻萃取：每天筛出值得看的 3 条 AI 资讯，支持简报阅读与原文回看。"

DASHSCOPE_API_KEY = get_dashscope_api_key()
DASHSCOPE_BASE_URL = (
    os.getenv("DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1").strip()
    or "https://dashscope.aliyuncs.com/compatible-mode/v1"
)
PUSH_REWRITE_ENABLED = str(os.getenv("PUSH_REWRITE_ENABLED", "1")).strip().lower() not in {
    "0",
    "false",
    "off",
    "no",
}
PUSH_REWRITE_MODEL = os.getenv("PUSH_REWRITE_MODEL", "qwen-plus").strip() or "qwen-plus"
PUSH_REWRITE_TEMPERATURE = float(os.getenv("PUSH_REWRITE_TEMPERATURE", "0.7").strip() or "0.7")
PUSH_REWRITE_PROMPT_VERSION = "selected-brief-v2"

_rewrite_llm_client: Optional[AsyncOpenAI] = None


def is_push_configured() -> bool:
    return bool(UNIPUSH_HTTP_ENDPOINT)


def _can_use_rewrite_agent() -> bool:
    return PUSH_REWRITE_ENABLED and bool(DASHSCOPE_API_KEY)


def _get_rewrite_llm_client() -> AsyncOpenAI:
    global _rewrite_llm_client
    if _rewrite_llm_client is None:
        _rewrite_llm_client = AsyncOpenAI(
            api_key=DASHSCOPE_API_KEY,
            base_url=DASHSCOPE_BASE_URL,
        )
    return _rewrite_llm_client


def _clip_text(value: str, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _normalize_text_list(value) -> List[str]:
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    text = str(value or "").strip()
    return [text] if text else []


def _normalize_tags(value) -> List[str]:
    tags = _normalize_text_list(value)
    deduped: List[str] = []
    seen = set()
    for tag in tags:
        lowered = tag.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(_clip_text(tag, 16))
    return deduped[:6]


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _build_fallback_question_headline(headline: str, limit: int = 28) -> str:
    text = str(headline or "").strip()
    if not text:
        return "今天这条 AI 消息，到底在说啥？"
    if len(text) <= 16 and not any(mark in text for mark in ("？", "?", "！", "!")):
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

    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]

    value = json.loads(raw)
    return value if isinstance(value, dict) else {}


def _extract_article_brief(article: Dict) -> Dict:
    stored_brief = str(article.get("brief_json", "") or "").strip()
    if not stored_brief:
        return {}
    try:
        brief = json.loads(stored_brief)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return brief if isinstance(brief, dict) else {}


def _extract_article_overview(article: Dict) -> str:
    brief = _extract_article_brief(article)
    lead = str(brief.get("lead", "") or "").strip()
    if lead:
        return lead

    paragraphs = brief.get("paragraphs") or []
    if isinstance(paragraphs, list):
        for paragraph in paragraphs:
            text = str(paragraph or "").strip()
            if text:
                return text

    summary = str(article.get("summary", "") or "").strip()
    if summary:
        return summary

    content = str(article.get("content", "") or "").strip()
    if not content:
        return ""
    return _clip_text(content, 140)


def _normalize_body_paragraphs(article: Dict) -> List[str]:
    brief = _extract_article_brief(article)
    paragraphs = brief.get("paragraphs") or []
    normalized = _normalize_text_list(paragraphs)
    if normalized:
        return normalized

    overview = _extract_article_overview(article)
    return [overview] if overview else []


def _extract_article_tags(article: Dict) -> List[str]:
    brief = _extract_article_brief(article)
    tags = _normalize_tags(brief.get("tags"))
    if tags:
        return tags

    source = str(article.get("source", "") or "").strip()
    if source:
        return [_clip_text(source, 16), "AI资讯"]
    return ["AI资讯"]


def _build_article_card(article: Dict, board: str, rank: int) -> Dict:
    return {
        "board": board,
        "rank": rank,
        "newsId": article.get("id"),
        "title": article.get("title", ""),
        "source": article.get("source", ""),
        "url": article.get("url", ""),
        "coverUrl": article.get("cover_url", ""),
        "overview": _extract_article_overview(article),
        "summary": str(article.get("summary", "") or "").strip(),
        "publishedAt": article.get("published_at"),
        "score": article.get("total_score", 0),
    }


async def build_daily_highlights(limit: int = 2) -> Dict:
    main_articles = await get_top_articles(limit=limit, board="business")
    sub_articles = await get_top_articles(limit=limit, board="personal")

    main_list = [
        _build_article_card(article, "main", index)
        for index, article in enumerate(main_articles[:limit], 1)
    ]
    sub_list = [
        _build_article_card(article, "sub", index)
        for index, article in enumerate(sub_articles[:limit], 1)
    ]

    top_titles = [item["title"] for item in (main_list[:1] + sub_list[:1]) if item.get("title")]
    if top_titles:
        body = "；".join(top_titles[:2])
        if len(body) > 56:
            body = _clip_text(body, 56)
    else:
        body = DEFAULT_PUSH_BODY

    return {
        "title": DEFAULT_PUSH_TITLE,
        "body": body or DEFAULT_PUSH_BODY,
        "route": DEFAULT_PUSH_ROUTE,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "boards": {
            "main": main_list,
            "sub": sub_list,
        },
    }


def _build_issue_item(article: Dict, issue_id: str, index: int) -> Dict:
    brief = dict(_extract_article_brief(article))
    headline = str(brief.get("headline") or article.get("title") or "").strip()
    warning = _extract_article_overview(article)
    body = _normalize_body_paragraphs(article)

    brief.setdefault("style", brief.get("style") or "selected_issue")
    brief["headline"] = headline
    brief["lead"] = str(brief.get("lead") or warning).strip()
    brief["paragraphs"] = body
    brief["tags"] = _normalize_tags(brief.get("tags")) or _extract_article_tags(article)
    if not isinstance(brief.get("sources"), list):
        article_url = str(article.get("url", "") or "").strip()
        if article_url:
            brief["sources"] = [
                {
                    "label": str(article.get("source", "") or "").strip() or "原文",
                    "url": article_url,
                }
            ]

    return {
        "id": f"{issue_id}-{index}",
        "newsId": int(article.get("id") or 0),
        "source": str(article.get("source", "") or "").strip(),
        "headline": headline,
        "warning": warning,
        "articleUrl": str(article.get("url", "") or "").strip(),
        "coverImage": str(article.get("cover_url", "") or "").strip(),
        "expandedBody": body,
        "brief": brief,
    }


def _build_selected_issue(articles: List[Dict], created_by: str = "") -> Dict:
    issue_date = datetime.now().strftime("%Y-%m-%d")
    issue_id = issue_date
    items = [
        _build_issue_item(article, issue_id, index)
        for index, article in enumerate(articles[:3], 1)
    ]
    return {
        "id": issue_id,
        "date": issue_date,
        "selectionCount": len(articles),
        "title": f"{issue_date} AI趣闻萃取 / 三条速览",
        "subtitle": DEFAULT_ISSUE_SUBTITLE,
        "footer": DEFAULT_ISSUE_FOOTER,
        "createdBy": str(created_by or "").strip(),
        "items": items,
    }


def _format_issue_title(issue_date: str, title: str, fallback: str) -> str:
    normalized_title = str(title or "").strip()
    if not normalized_title:
        return fallback
    if issue_date and issue_date in normalized_title:
        return normalized_title
    return f"{issue_date} {normalized_title}".strip()


def _build_rewrite_prompt(articles: List[Dict]) -> str:
    materials = []
    for article in articles[:3]:
        materials.append(
            {
                "newsId": int(article.get("id") or 0),
                "title": str(article.get("title", "") or "").strip(),
                "source": str(article.get("source", "") or "").strip(),
                "summary": _clip_text(str(article.get("summary", "") or ""), 220),
                "overview": _extract_article_overview(article),
                "paragraphs": _normalize_body_paragraphs(article)[:4],
                "tags": _extract_article_tags(article),
            }
        )

    schema = {
        "issueTitle": "字符串，栏目标题，适合页面顶部，不要带多余解释",
        "pushTitle": "字符串，系统通知标题，8-18字",
        "pushBody": "字符串，系统通知正文，18-36字",
        "items": [
            {
                "newsId": "整数，必须与输入完全一致",
                "headline": "字符串，手机卡片标题，12-26字，更像讲给小白听的版本",
                "warning": "字符串，一句话说明为什么值得看，24-48字",
                "expandedBody": ["字符串，每条4-6段，每段1-2句短句，像讲故事一样解释清楚"],
                "tags": ["字符串，2-4个短标签"],
            }
        ],
    }

    return (
        "你是 AI 栏目“AI趣闻萃取”的改写编辑。\n"
        "请把 3 条 AI 新闻改写成更有趣、抓眼球、但不失真的普通人可读版本。\n"
        "必须遵守下面规则：\n"
        "1. 绝不编造事实，不补不存在的人名、时间、数字、结论。\n"
        "2. 语言可以轻松、有画面感，但不要油腻，不要空话，不要过度标题党。\n"
        "3. 面向第一次接触 AI 的用户，尽量把术语说成人话，幼儿园也能大致看懂。\n"
        "4. expandedBody 每条 4-6 段，每段 1-2 句短句，总体要比输入更完整。\n"
        "5. 正文尽量遵循这个顺序：先说发生了什么，再说原文里的关键点，再说为什么普通人值得看，最后提醒去看原文。\n"
        "6. warning 是一句“这条新闻最值得注意的点”。\n"
        "7. 必须原样返回输入里的 3 个 newsId，不许新增、遗漏或改动。\n"
        "8. 只能输出 JSON，不要 Markdown，不要解释。\n\n"
        f"返回 JSON Schema：{json.dumps(schema, ensure_ascii=False)}\n\n"
        f"输入素材：{json.dumps(materials, ensure_ascii=False)}"
    )


def _fallback_rewrite_issue_item(base_item: Dict) -> Dict:
    base_brief = dict(base_item.get("brief") or {})
    original_headline = str(base_item.get("headline") or "").strip()
    original_warning = str(base_item.get("warning") or "").strip()
    original_body = _normalize_text_list(base_item.get("expandedBody"))
    source = str(base_item.get("source") or "").strip()

    facts = _dedupe_preserve_order(
        [
            _condense_fact_text(original_warning, 84),
            *[_condense_fact_text(paragraph, 84) for paragraph in original_body[:3]],
            _condense_fact_text(str(base_brief.get("lead") or "").strip(), 84),
            _condense_fact_text(original_headline, 48),
        ]
    )
    main_fact = facts[0] if facts else _condense_fact_text(original_headline, 48)
    extra_fact = facts[1] if len(facts) > 1 else ""
    reminder = _condense_fact_text(original_warning or main_fact or original_headline, 60)
    source_hint = f"这条消息来自 {source}，所以更适合把它当成一个值得继续观察的新信号。" if source else ""

    expanded_body = _dedupe_preserve_order(
        [
            f"先把这条消息翻成人话，它主要在说：{main_fact}",
            "别急着记专业词，你可以先把它理解成 AI 圈又出现了一个值得围观的新变化。",
            f"原文里更具体的一点是：{extra_fact}" if extra_fact else "",
            "对普通人来说，先知道“这件事正在发生，而且值得多看一眼”就已经够用了。",
            source_hint,
            f"如果你只想记住一句最核心的话，那就是：{reminder}",
            "想把细节看得更准一点，最稳的办法还是点开原文，把完整上下文看一遍。",
        ]
    )
    expanded_body = expanded_body[:6]
    headline = _build_fallback_question_headline(original_headline, 28)
    warning = _clip_text(
        _condense_fact_text(original_warning, 84)
        or f"先别被专业词劝退，这条新闻其实在说：{main_fact or _condense_fact_text(original_headline, 48)}",
        120,
    )

    brief = {
        **base_brief,
        "style": "push_fallback_rewrite",
        "headline": headline,
        "lead": warning,
        "paragraphs": expanded_body,
        "tags": _normalize_tags(base_brief.get("tags")) or ["AI趣闻", "今日热榜"],
        "rewriteApplied": False,
        "rewriteMode": "push_fallback",
        "originalHeadline": original_headline,
        "originalLead": original_warning,
        "originalParagraphs": original_body,
    }
    if not isinstance(brief.get("sources"), list):
        article_url = str(base_item.get("articleUrl", "") or "").strip()
        if article_url:
            brief["sources"] = [
                {
                    "label": str(base_item.get("source", "") or "").strip() or "原文",
                    "url": article_url,
                }
            ]

    return {
        **base_item,
        "headline": headline,
        "warning": warning,
        "expandedBody": expanded_body,
        "brief": brief,
    }


def _merge_rewritten_item(base_item: Dict, rewrite_item: Optional[Dict]) -> Dict:
    base_brief = dict(base_item.get("brief") or {})
    original_headline = str(base_item.get("headline", "") or "").strip()
    original_warning = str(base_item.get("warning", "") or "").strip()
    original_body = _normalize_text_list(base_item.get("expandedBody"))

    rewrite_item = rewrite_item if isinstance(rewrite_item, dict) else {}
    rewritten_headline = _clip_text(str(rewrite_item.get("headline", "") or "").strip(), 48)
    rewritten_warning = _clip_text(str(rewrite_item.get("warning", "") or "").strip(), 120)
    rewritten_body = _normalize_text_list(rewrite_item.get("expandedBody"))
    rewritten_tags = _normalize_tags(rewrite_item.get("tags"))

    if rewrite_item and len(rewritten_body) < 4:
        return _fallback_rewrite_issue_item(base_item)

    headline = rewritten_headline or original_headline
    warning = rewritten_warning or original_warning
    expanded_body = rewritten_body or original_body

    brief = {
        **base_brief,
        "style": "push_rewrite" if rewrite_item else str(base_brief.get("style") or "selected_issue"),
        "headline": headline,
        "lead": warning,
        "paragraphs": expanded_body,
        "tags": rewritten_tags or _normalize_tags(base_brief.get("tags")) or ["AI趣闻", "今日热榜"],
        "rewriteApplied": bool(rewrite_item),
        "rewriteModel": PUSH_REWRITE_MODEL if rewrite_item else str(base_brief.get("rewriteModel") or "").strip(),
        "rewritePromptVersion": (
            PUSH_REWRITE_PROMPT_VERSION
            if rewrite_item
            else str(base_brief.get("rewritePromptVersion") or "").strip()
        ),
        "originalHeadline": original_headline,
        "originalLead": original_warning,
        "originalParagraphs": original_body,
    }

    if not isinstance(brief.get("sources"), list):
        article_url = str(base_item.get("articleUrl", "") or "").strip()
        if article_url:
            brief["sources"] = [
                {
                    "label": str(base_item.get("source", "") or "").strip() or "原文",
                    "url": article_url,
                }
            ]

    return {
        **base_item,
        "headline": headline,
        "warning": warning,
        "expandedBody": expanded_body,
        "brief": brief,
    }


async def _rewrite_selected_issue_content(articles: List[Dict], base_issue: Dict) -> Dict:
    fallback_issue = {
        **base_issue,
        "items": [_fallback_rewrite_issue_item(item) for item in base_issue.get("items") or []],
    }
    fallback = {
        "issue": fallback_issue,
        "pushTitle": DEFAULT_SELECTED_PUSH_TITLE,
        "pushBody": DEFAULT_SELECTED_PUSH_BODY,
        "rewriteApplied": False,
        "rewriteFallbackReason": "",
    }

    if not _can_use_rewrite_agent():
        fallback["rewriteFallbackReason"] = "rewrite_disabled"
        return fallback

    prompt = _build_rewrite_prompt(articles)
    try:
        response = await _get_rewrite_llm_client().chat.completions.create(
            model=PUSH_REWRITE_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=PUSH_REWRITE_TEMPERATURE,
        )
        content = str(response.choices[0].message.content or "").strip()
        payload = _extract_json_object(content)
    except Exception as exc:
        logger.warning("selected issue rewrite failed: %s", exc)
        fallback["rewriteFallbackReason"] = f"rewrite_error:{exc}"
        return fallback

    payload_items = payload.get("items")
    if not isinstance(payload_items, list):
        fallback["rewriteFallbackReason"] = "rewrite_invalid_payload"
        return fallback

    rewrite_by_id: Dict[int, Dict] = {}
    for item in payload_items:
        if not isinstance(item, dict):
            continue
        try:
            news_id = int(item.get("newsId") or 0)
        except (TypeError, ValueError):
            continue
        if news_id > 0:
            rewrite_by_id[news_id] = item

    rewritten_items = []
    rewritten_count = 0
    for base_item in base_issue.get("items") or []:
        rewrite_item = rewrite_by_id.get(int(base_item.get("newsId") or 0))
        merged_item = _merge_rewritten_item(base_item, rewrite_item)
        if merged_item.get("brief", {}).get("rewriteApplied"):
            rewritten_count += 1
        rewritten_items.append(merged_item)

    rewritten_issue = {
        **base_issue,
        "items": rewritten_items,
    }

    issue_title = _format_issue_title(
        str(base_issue.get("date") or "").strip(),
        str(payload.get("issueTitle") or "").strip(),
        str(base_issue.get("title") or "").strip(),
    )
    if rewritten_count > 0 and issue_title:
        rewritten_issue["title"] = issue_title

    push_title = _clip_text(str(payload.get("pushTitle") or "").strip(), 24) or DEFAULT_SELECTED_PUSH_TITLE
    push_body = _clip_text(str(payload.get("pushBody") or "").strip(), 60) or DEFAULT_SELECTED_PUSH_BODY

    return {
        "issue": rewritten_issue,
        "pushTitle": push_title,
        "pushBody": push_body,
        "rewriteApplied": rewritten_count > 0,
        "rewriteFallbackReason": "" if rewritten_count > 0 else "rewrite_empty",
    }


async def create_selected_issue(article_ids: List[int], created_by: str = "") -> Dict:
    normalized_ids = []
    for article_id in article_ids:
        try:
            normalized_ids.append(int(article_id))
        except (TypeError, ValueError):
            continue

    normalized_ids = list(dict.fromkeys(normalized_ids))
    if len(normalized_ids) != 3:
        raise ValueError("exactly 3 articleIds are required")

    articles = await get_articles_by_ids(normalized_ids)
    if len(articles) != 3:
        raise ValueError("some selected articles were not found")

    base_issue = _build_selected_issue(articles, created_by=created_by)
    rewritten = await _rewrite_selected_issue_content(articles, base_issue)
    issue_to_save = rewritten["issue"]

    stored_issue = await save_push_brief_issue(issue_to_save, created_by=created_by)
    stored_issue["pushTitle"] = rewritten["pushTitle"]
    stored_issue["pushBody"] = rewritten["pushBody"]
    stored_issue["rewriteApplied"] = bool(rewritten.get("rewriteApplied"))
    stored_issue["rewriteFallbackReason"] = str(rewritten.get("rewriteFallbackReason") or "").strip()
    if stored_issue["rewriteApplied"]:
        stored_issue["rewriteModel"] = PUSH_REWRITE_MODEL
        stored_issue["rewritePromptVersion"] = PUSH_REWRITE_PROMPT_VERSION
    return stored_issue


def _normalize_client_ids(client_ids: List[str]) -> List[str]:
    normalized_ids = []
    for client_id in client_ids:
        value = str(client_id or "").strip()
        if value and value.lower() not in INVALID_PUSH_CLIENT_IDS:
            normalized_ids.append(value)
    return list(dict.fromkeys(normalized_ids))


def _build_bridge_request(
    client_ids: List[str],
    title: str,
    body: str,
    route: str,
    extra_payload: Optional[Dict] = None,
) -> Dict:
    payload = {
        "type": "daily_news",
        "route": route or DEFAULT_PUSH_ROUTE,
        "date": datetime.now().strftime("%Y-%m-%d"),
    }
    if extra_payload:
        payload.update(extra_payload)

    return {
        "requestId": uuid.uuid4().hex,
        "clientIds": client_ids,
        "title": title,
        "body": body,
        "route": payload["route"],
        "payload": payload,
    }


def _normalize_bridge_response(response_data: Dict, client_ids: List[str]) -> Dict:
    payload = response_data
    if isinstance(response_data.get("data"), dict):
        payload = response_data["data"]
    elif "statusCode" in response_data and "body" in response_data:
        raw_body = response_data.get("body")
        if isinstance(raw_body, str):
            try:
                payload = json.loads(raw_body)
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {"success": False, "error": raw_body}
        elif isinstance(raw_body, dict):
            payload = raw_body
        else:
            payload = {"success": False, "error": "uni-push bridge returned empty body"}

        status_code = int(response_data.get("statusCode") or 200)
        if status_code >= 400 and isinstance(payload, dict):
            payload["success"] = False

    taskid_map = {}
    raw_taskid_map = payload.get("taskid")
    if isinstance(raw_taskid_map, dict):
        taskid_map = {
            str(client_id or "").strip(): str(status or "").strip()
            for client_id, status in raw_taskid_map.items()
            if str(client_id or "").strip()
        }

    results = payload.get("results")
    if not isinstance(results, list):
        normalized_results = []
        for client_id in client_ids:
            raw_status = taskid_map.get(client_id, "")
            if raw_status:
                lowered_status = raw_status.lower()
                success = lowered_status.startswith("success")
                item = {
                    "clientId": client_id,
                    "success": success,
                    "rawStatus": raw_status,
                }
                if not success:
                    item["error"] = raw_status
                normalized_results.append(item)
                continue

            success = bool(payload.get("success", True))
            normalized_results.append({"clientId": client_id, "success": success})
        results = normalized_results
    else:
        normalized_results = []
        for item in results:
            if isinstance(item, dict):
                normalized_results.append(
                    {
                        "clientId": str(item.get("clientId") or item.get("client_id") or "").strip(),
                        "success": bool(item.get("success")),
                        **({"rawStatus": item.get("rawStatus")} if item.get("rawStatus") is not None else {}),
                        **({"response": item.get("response")} if item.get("response") is not None else {}),
                        **({"error": item.get("error")} if item.get("error") is not None else {}),
                    }
                )
        results = normalized_results

    sent = payload.get("sent")
    if not isinstance(sent, int):
        sent = sum(1 for item in results if item.get("success"))

    success = bool(payload.get("success", sent > 0))
    return {
        "success": success,
        "sent": sent,
        "results": results,
        "response": response_data,
    }


async def send_notification_to_client_ids(
    client_ids: List[str],
    title: str,
    body: str,
    route: str = "",
    extra_payload: Optional[Dict] = None,
) -> Dict:
    normalized_ids = _normalize_client_ids(client_ids)
    if not normalized_ids:
        return {"success": True, "sent": 0, "results": []}
    if not is_push_configured():
        raise RuntimeError("uni-push bridge is not configured")

    request_payload = _build_bridge_request(
        client_ids=normalized_ids,
        title=title,
        body=body,
        route=route,
        extra_payload=extra_payload,
    )

    headers = {"Content-Type": "application/json"}
    if UNIPUSH_HTTP_AUTH_TOKEN:
        headers["x-push-bridge-secret"] = UNIPUSH_HTTP_AUTH_TOKEN

    timeout = httpx.Timeout(UNIPUSH_HTTP_TIMEOUT, connect=min(10.0, UNIPUSH_HTTP_TIMEOUT))
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                UNIPUSH_HTTP_ENDPOINT,
                headers=headers,
                json=request_payload,
            )
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text.strip()
        raise RuntimeError(f"uni-push bridge http {exc.response.status_code}: {detail or exc}") from exc
    except httpx.RequestError as exc:
        raise RuntimeError(f"uni-push bridge unreachable: {exc}") from exc
    except ValueError as exc:
        raise RuntimeError("uni-push bridge returned invalid JSON") from exc

    if isinstance(data, dict) and data.get("success") is False:
        raise RuntimeError(str(data.get("error") or data.get("message") or "uni-push bridge failed"))

    result = _normalize_bridge_response(data if isinstance(data, dict) else {}, normalized_ids)
    logger.info(
        "push bridge request finished sent=%s total=%s endpoint=%s",
        result["sent"],
        len(normalized_ids),
        UNIPUSH_HTTP_ENDPOINT,
    )
    return result


async def send_daily_highlights_to_registered_devices(limit: int = 2) -> Dict:
    devices = await list_push_devices(platform="android", push_enabled_only=True)
    client_ids = [item.get("client_id", "") for item in devices if item.get("client_id")]
    if not client_ids:
        logger.info("daily push skipped: no registered android devices")
        return {"success": True, "sent": 0, "reason": "no_devices"}

    digest = await build_daily_highlights(limit=limit)
    if not is_push_configured():
        logger.warning("daily push skipped: uni-push bridge is not configured")
        return {
            "success": True,
            "sent": 0,
            "reason": "push_unconfigured",
            "digest": digest,
        }

    result = await send_notification_to_client_ids(
        client_ids=client_ids,
        title=digest["title"],
        body=digest["body"],
        route=digest["route"],
        extra_payload={"date": digest["date"]},
    )
    result["digest"] = digest
    return result


async def send_selected_issue_to_registered_devices(article_ids: List[int], created_by: str = "") -> Dict:
    issue = await create_selected_issue(article_ids, created_by=created_by)
    devices = await list_push_devices(platform="android", push_enabled_only=True)
    client_ids = [item.get("client_id", "") for item in devices if item.get("client_id")]

    if not client_ids:
        logger.info("selected issue saved but push skipped: no registered android devices")
        return {
            "success": True,
            "sent": 0,
            "reason": "no_devices",
            "issue": issue,
        }

    if not is_push_configured():
        logger.warning("selected issue saved but push skipped: uni-push bridge is not configured")
        return {
            "success": True,
            "sent": 0,
            "reason": "push_unconfigured",
            "issue": issue,
        }

    result = await send_notification_to_client_ids(
        client_ids=client_ids,
        title=str(issue.get("pushTitle") or DEFAULT_SELECTED_PUSH_TITLE),
        body=str(issue.get("pushBody") or DEFAULT_SELECTED_PUSH_BODY),
        route=DEFAULT_PUSH_ROUTE,
        extra_payload={
            "type": "news_brief_issue",
            "issueId": issue["id"],
            "date": issue["date"],
        },
    )
    result["issue"] = issue
    return result
