import os
import re
import uuid
from typing import Any, Dict, List, Optional, Sequence

from openai import AsyncOpenAI

from chat_prompts import (
    build_answer_prompt,
    build_intent_prompt,
    extract_json_object,
    normalize_message_text,
)
from db import get_news_chat_session, list_news_chat_sessions, upsert_news_chat_session
from env_loader import get_dashscope_api_key, load_crawl_env
from retrieval_service import build_citations, retrieve_news_articles


load_crawl_env()

_CHAT_API_KEY = ""
_CHAT_BASE_URL = ""
_CHAT_MODEL = ""
_CHAT_CLIENT: Optional[AsyncOpenAI] = None
_ALLOWED_INTENTS = {
    "news_qa",
    "latest_news",
    "summary",
    "compare",
    "recommend",
    "follow_up",
    "unknown",
}
_SOURCE_KEY_HINTS = {
    "hacker news": "hacker_news",
    "hn": "hacker_news",
    "openai": "openai_blog",
    "techcrunch": "techcrunch_ai",
    "the verge": "the_verge_ai",
    "verge": "the_verge_ai",
    "ars": "ars_technica_ai",
    "arstechnica": "ars_technica_ai",
    "mit": "mit_tech_review",
    "mit technology review": "mit_tech_review",
    "github": "github_blog",
    "google": "google_blog",
    "anthropic": "anthropic_blog",
}


def _normalize_source_keys(values: Sequence[Any]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values or []:
        key = str(value or "").strip()
        if not key:
            continue
        normalized = key.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _guess_source_keys(message: str) -> List[str]:
    text = str(message or "").lower()
    keys: List[str] = []
    for needle, source_key in _SOURCE_KEY_HINTS.items():
        if needle in text and source_key not in keys:
            keys.append(source_key)
    return keys


def _fallback_intent(message: str, history: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    text = str(message or "").strip()
    lower = text.lower()
    recent_context = " ".join(str(item.get("content") or "") for item in list(history)[-4:])
    recent_text = f"{recent_context} {text}".strip().lower()

    if any(token in lower for token in ("最新", "最近", "今天", "本周", "这周", "近期")):
        intent = "latest_news"
        days = 3 if any(token in lower for token in ("今天", "最新")) else 7
    elif any(token in lower for token in ("总结", "概括", "梳理", "overview", "summary")):
        intent = "summary"
        days = 14
    elif any(token in lower for token in ("比较", "对比", "区别", "vs", "versus")):
        intent = "compare"
        days = 14
    elif any(token in lower for token in ("推荐", "值得看", "关注", "挑选")):
        intent = "recommend"
        days = 14
    elif history and any(token in recent_text for token in ("它", "这个", "上面", "刚才", "前面", "那条")):
        intent = "follow_up"
        days = 7
    else:
        intent = "news_qa"
        days = 7

    source_keys = _guess_source_keys(text)
    query = text
    if intent == "latest_news":
        query = ""
    elif intent == "follow_up" and history:
        query = str(history[-1].get("content") or text).strip()

    return {
        "intent": intent,
        "query": query,
        "source_keys": source_keys,
        "days": days,
        "need_clarification": False,
        "clarification_question": "",
        "answer_style": "concise",
        "confidence": 0.45,
        "router": "fallback",
    }


def _resolve_chat_config() -> Dict[str, str]:
    news_chat_api_key = str(os.getenv("NEWS_CHAT_API_KEY", "") or "").strip()
    dashscope_api_key = get_dashscope_api_key()
    deepseek_api_key = str(os.getenv("DEEPSEEK_API_KEY", "") or "").strip()

    provider = ""
    api_key = ""
    if news_chat_api_key:
        provider = "news_chat"
        api_key = news_chat_api_key
    elif dashscope_api_key:
        provider = "dashscope"
        api_key = dashscope_api_key
    elif deepseek_api_key:
        provider = "deepseek"
        api_key = deepseek_api_key

    if not api_key:
        return {"api_key": "", "base_url": "", "model": ""}

    if str(os.getenv("NEWS_CHAT_BASE_URL", "") or "").strip():
        base_url = str(os.getenv("NEWS_CHAT_BASE_URL", "")).strip()
    elif provider == "dashscope":
        base_url = str(
            os.getenv(
                "NEWS_CHAT_BASE_URL",
                "https://dashscope.aliyuncs.com/compatible-mode/v1",
            )
        ).strip()
    elif provider == "deepseek":
        base_url = str(
            os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
        ).strip()
    else:
        base_url = str(
            os.getenv("NEWS_CHAT_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        ).strip()

    model = (
        str(os.getenv("NEWS_CHAT_MODEL", "") or "").strip()
        or str(os.getenv("LLM_CHAT_MODEL", "") or "").strip()
        or (
            "deepseek-chat"
            if provider == "deepseek"
            else "qwen-plus"
        )
    )
    return {"api_key": api_key, "base_url": base_url, "model": model}


def _get_chat_client() -> Optional[AsyncOpenAI]:
    global _CHAT_API_KEY, _CHAT_BASE_URL, _CHAT_MODEL, _CHAT_CLIENT
    config = _resolve_chat_config()
    if not config["api_key"]:
        return None
    if (
        _CHAT_CLIENT is None
        or _CHAT_API_KEY != config["api_key"]
        or _CHAT_BASE_URL != config["base_url"]
        or _CHAT_MODEL != config["model"]
    ):
        _CHAT_API_KEY = config["api_key"]
        _CHAT_BASE_URL = config["base_url"]
        _CHAT_MODEL = config["model"]
        _CHAT_CLIENT = AsyncOpenAI(api_key=_CHAT_API_KEY, base_url=_CHAT_BASE_URL)
    return _CHAT_CLIENT


def _clip_history(history: Sequence[Dict[str, Any]], limit: int = 12) -> List[Dict[str, Any]]:
    clipped: List[Dict[str, Any]] = []
    for item in list(history or [])[-limit:]:
        role = str(item.get("role") or "").strip() or "user"
        content = normalize_message_text(item.get("content") or "", 1000)
        if content:
            clipped.append({"role": role, "content": content})
    return clipped


def _build_session_title(message: str) -> str:
    text = re.sub(r"\s+", " ", str(message or "")).strip()
    if not text:
        return "新闻对话"
    return text[:18]


async def _route_intent(
    message: str,
    history: Sequence[Dict[str, Any]],
    client: Optional[AsyncOpenAI],
    model: str,
) -> Dict[str, Any]:
    fallback = _fallback_intent(message, history)
    if client is None or not model:
        return fallback

    prompt = build_intent_prompt(message, history, sorted(_SOURCE_KEY_HINTS.values()))
    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "你是一个只输出 JSON 的意图识别器。",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        content = str(response.choices[0].message.content or "").strip()
        parsed = extract_json_object(content)
        intent = str(parsed.get("intent") or "").strip()
        if intent not in _ALLOWED_INTENTS:
            return fallback
        source_keys = _normalize_source_keys(parsed.get("source_keys") or [])
        source_keys.extend(_guess_source_keys(message))
        deduped_source_keys = _normalize_source_keys(source_keys)
        query = str(parsed.get("query") or "").strip() or message
        days = parsed.get("days")
        try:
            days = int(days)
        except (TypeError, ValueError):
            days = fallback["days"]
        return {
            "intent": intent,
            "query": query if intent != "latest_news" else "",
            "source_keys": deduped_source_keys,
            "days": max(1, min(30, days or fallback["days"])),
            "need_clarification": bool(parsed.get("need_clarification", False)),
            "clarification_question": str(parsed.get("clarification_question") or "").strip(),
            "answer_style": str(parsed.get("answer_style") or "concise").strip() or "concise",
            "confidence": float(parsed.get("confidence", 0.0) or 0.0),
            "router": "llm",
        }
    except Exception:
        return fallback


def _build_fallback_answer(message: str, intent: Dict[str, Any], matches: Sequence[Dict[str, Any]]) -> str:
    if not matches:
        return "没有找到足够证据来直接回答这个问题。你可以把范围缩小到具体主题、来源或时间段。"

    if intent.get("intent") == "latest_news":
        lead = "最近的相关新闻如下："
    elif intent.get("intent") == "compare":
        lead = "基于现有新闻，能对比出的重点如下："
    elif intent.get("intent") == "recommend":
        lead = "更值得优先看的新闻如下："
    else:
        lead = "根据新闻库检索到的证据，回答如下："

    lines = [lead]
    for idx, item in enumerate(matches[:4], 1):
        title = str(item.get("title") or "").strip()
        source = str(item.get("source") or "").strip()
        snippet = str(item.get("snippet") or item.get("summary") or "").strip()
        if snippet and len(snippet) > 90:
            snippet = snippet[:90].rstrip() + "..."
        lines.append(f"{idx}. {title}（{source}）: {snippet} [{idx}]")
    return "\n".join(lines)


async def _generate_answer(
    message: str,
    history: Sequence[Dict[str, Any]],
    intent: Dict[str, Any],
    citations: Sequence[Dict[str, Any]],
    client: Optional[AsyncOpenAI],
    model: str,
) -> str:
    if client is None or not model:
        return _build_fallback_answer(message, intent, citations)

    prompt = build_answer_prompt(message, intent, citations, history)
    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "你是一个只基于证据回答问题的新闻助手。",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        text = str(response.choices[0].message.content or "").strip()
        return text or _build_fallback_answer(message, intent, citations)
    except Exception:
        return _build_fallback_answer(message, intent, citations)


class NewsChatService:
    async def list_sessions(self, limit: int = 20) -> List[Dict[str, Any]]:
        sessions = await list_news_chat_sessions(limit=limit)
        result: List[Dict[str, Any]] = []
        for item in sessions:
            history = item.get("data_json", [])
            last_message = ""
            if isinstance(history, list) and history:
                for entry in reversed(history):
                    if isinstance(entry, dict) and str(entry.get("content") or "").strip():
                        last_message = str(entry.get("content") or "").strip()
                        break
            result.append(
                {
                    "sessionId": item.get("id"),
                    "title": item.get("title", ""),
                    "historyCount": len(history) if isinstance(history, list) else 0,
                    "lastMessage": last_message,
                    "createdAt": item.get("created_at"),
                    "updatedAt": item.get("updated_at"),
                }
            )
        return result

    async def preview_search(
        self,
        *,
        query: str,
        limit: int = 8,
        source_keys: Optional[Sequence[str]] = None,
        days: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_query = normalize_message_text(query, 1000)
        if not normalized_query:
            raise ValueError("query is required")
        retrieval = await retrieve_news_articles(
            normalized_query,
            source_keys=source_keys,
            days=days,
            limit=limit,
        )
        matches = retrieval["matches"]
        return {
            "query": normalized_query,
            "retrieval": {
                "mode": retrieval.get("retrievalMode"),
                "fallbackUsed": retrieval.get("fallbackUsed"),
                "sourceKeys": retrieval.get("sourceKeys", []),
                "days": retrieval.get("days"),
                "tokens": retrieval.get("tokens", []),
            },
            "matches": matches,
            "citations": build_citations(matches),
        }

    async def handle_message(
        self,
        *,
        message: str,
        session_id: str = "",
        history: Optional[Sequence[Dict[str, Any]]] = None,
        limit: int = 6,
        source_keys: Optional[Sequence[str]] = None,
        days: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_message = normalize_message_text(message, 1000)
        if not normalized_message:
            raise ValueError("message is required")

        session_id = str(session_id or "").strip() or str(uuid.uuid4())
        stored_session = await get_news_chat_session(session_id)
        stored_history = stored_session.get("data_json", []) if stored_session else []
        merged_history = _clip_history(history or stored_history)

        chat_client = _get_chat_client()
        model = _CHAT_MODEL or _resolve_chat_config().get("model", "")
        intent = await _route_intent(normalized_message, merged_history, chat_client, model)

        if source_keys:
            intent["source_keys"] = _normalize_source_keys(list(intent.get("source_keys") or []) + list(source_keys))
        if days is not None:
            try:
                intent["days"] = max(1, int(days))
            except (TypeError, ValueError):
                pass

        retrieval = await retrieve_news_articles(
            intent.get("query") or normalized_message,
            source_keys=intent.get("source_keys") or None,
            days=intent.get("days"),
            limit=limit,
        )
        matches = retrieval["matches"]
        citations = build_citations(matches)
        answer = await _generate_answer(
            normalized_message,
            merged_history,
            intent,
            citations,
            chat_client,
            model,
        )

        new_history = list(merged_history)
        new_history.append({"role": "user", "content": normalized_message})
        new_history.append({"role": "assistant", "content": answer})
        new_history = _clip_history(new_history, limit=16)

        title = str(stored_session.get("title") or "").strip() if stored_session else ""
        if not title:
            title = _build_session_title(normalized_message)

        await upsert_news_chat_session(session_id, title=title, data_json=new_history)

        return {
            "sessionId": session_id,
            "title": title,
            "message": normalized_message,
            "answer": answer,
            "intent": intent,
            "retrieval": {
                "mode": retrieval.get("retrievalMode"),
                "fallbackUsed": retrieval.get("fallbackUsed"),
                "sourceKeys": retrieval.get("sourceKeys", []),
                "days": retrieval.get("days"),
                "tokens": retrieval.get("tokens", []),
            },
            "citations": citations,
            "matches": matches,
            "history": new_history,
            "model": model,
            "usedLlm": bool(chat_client and model),
        }

    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        session_id = str(session_id or "").strip()
        if not session_id:
            return None
        session = await get_news_chat_session(session_id)
        if not session:
            return None
        return {
            "sessionId": session.get("id"),
            "title": session.get("title", ""),
            "history": session.get("data_json", []),
            "createdAt": session.get("created_at"),
            "updatedAt": session.get("updated_at"),
        }


news_chat_service = NewsChatService()
