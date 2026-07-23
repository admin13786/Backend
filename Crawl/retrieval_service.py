import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from db import search_news_articles


_TOKEN_RE = re.compile(r"[0-9A-Za-z\u4e00-\u9fff]+")


def normalize_query_tokens(query: str, limit: int = 12) -> List[str]:
    tokens: List[str] = []
    seen = set()
    for raw in _TOKEN_RE.findall(str(query or "").lower()):
        token = re.sub(r"^[的了和与及吗呢吧啊]+|[的了和与及吗呢吧啊]+$", "", raw.strip())
        if len(token) < 2 or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
        if len(tokens) >= limit:
            break
    return tokens


def _clip_text(value: str, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _parse_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _recency_score(article: Dict[str, Any]) -> float:
    dt = _parse_dt(article.get("published_at") or article.get("created_at"))
    if not dt:
        return 0.2
    age_hours = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
    return 1.0 / (1.0 + age_hours / 72.0)


def _count_term(text: str, token: str) -> int:
    return str(text or "").lower().count(token)


def score_article_for_query(article: Dict[str, Any], tokens: Sequence[str]) -> Dict[str, Any]:
    title = str(article.get("title") or "")
    summary = str(article.get("summary") or "")
    content = str(article.get("content") or "")
    source = str(article.get("source") or "")
    source_key = str(article.get("source_key") or "")

    lexical = 0.0
    matched: List[str] = []
    for token in tokens:
        hit = (
            4.0 * _count_term(title, token)
            + 2.0 * _count_term(summary, token)
            + 1.0 * min(_count_term(content, token), 6)
            + 1.5 * _count_term(source, token)
            + 1.5 * _count_term(source_key, token)
        )
        if hit > 0:
            matched.append(token)
            lexical += hit

    if tokens:
        lexical = min(1.0, lexical / (len(tokens) * 4.0))
    else:
        lexical = 0.0

    editorial_score = max(0.0, min(1.0, float(article.get("total_score") or 0) / 100.0))
    heat_score = max(0.0, min(1.0, float(article.get("spread_heat") or 0) / 100.0))
    recency = _recency_score(article)

    if tokens:
        final_score = 0.58 * lexical + 0.22 * editorial_score + 0.12 * recency + 0.08 * heat_score
    else:
        final_score = 0.50 * editorial_score + 0.35 * recency + 0.15 * heat_score

    return {
        "score": round(final_score, 6),
        "lexicalScore": round(lexical, 6),
        "editorialScore": round(editorial_score, 6),
        "recencyScore": round(recency, 6),
        "heatScore": round(heat_score, 6),
        "matchedKeywords": matched,
    }


def _make_snippet(article: Dict[str, Any], tokens: Sequence[str], limit: int = 220) -> str:
    haystacks = [
        str(article.get("summary") or ""),
        str(article.get("content") or ""),
        str(article.get("title") or ""),
    ]
    for text in haystacks:
        normalized = re.sub(r"\s+", " ", text).strip()
        if not normalized:
            continue
        lower = normalized.lower()
        for token in tokens:
            idx = lower.find(token)
            if idx >= 0:
                start = max(0, idx - 70)
                end = min(len(normalized), idx + limit)
                prefix = "..." if start > 0 else ""
                suffix = "..." if end < len(normalized) else ""
                return prefix + normalized[start:end].strip() + suffix
    for text in haystacks:
        snippet = _clip_text(text, limit)
        if snippet:
            return snippet
    return ""


def _to_match(article: Dict[str, Any], rank: int, tokens: Sequence[str]) -> Dict[str, Any]:
    scores = score_article_for_query(article, tokens)
    return {
        "rank": rank,
        "id": int(article.get("id") or 0),
        "title": str(article.get("title") or "").strip(),
        "url": str(article.get("url") or "").strip(),
        "source": str(article.get("source") or "").strip(),
        "sourceKey": str(article.get("source_key") or "").strip(),
        "publishedAt": article.get("published_at"),
        "summary": _clip_text(str(article.get("summary") or ""), 280),
        "snippet": _make_snippet(article, tokens),
        "totalScore": float(article.get("total_score") or 0),
        **scores,
    }


async def retrieve_news_articles(
    query: str,
    *,
    source_keys: Optional[Sequence[str]] = None,
    days: Optional[int] = None,
    limit: int = 8,
) -> Dict[str, Any]:
    tokens = normalize_query_tokens(query)
    candidate_limit = max(30, min(120, int(limit or 8) * 6))
    candidates = await search_news_articles(
        query=query,
        source_keys=source_keys,
        days=days,
        limit=candidate_limit,
    )

    # If strict keyword filtering returns nothing, fall back to recent/high-score news
    # so the chatbot can still explain that no direct evidence was found.
    fallback_used = False
    if not candidates and tokens:
        fallback_used = True
        candidates = await search_news_articles(
            query="",
            source_keys=source_keys,
            days=days,
            limit=candidate_limit,
        )

    scored = [
        (score_article_for_query(article, tokens)["score"], int(article.get("id") or 0), article)
        for article in candidates
    ]
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)

    matches: List[Dict[str, Any]] = []
    seen_urls = set()
    for _, _, article in scored:
        url = str(article.get("url") or "").strip()
        if url and url in seen_urls:
            continue
        seen_urls.add(url)
        matches.append(_to_match(article, len(matches) + 1, tokens))
        if len(matches) >= max(1, int(limit or 8)):
            break

    return {
        "query": query,
        "tokens": tokens,
        "matches": matches,
        "fallbackUsed": fallback_used,
        "retrievalMode": "hybrid_sql_keyword_score_recency",
        "sourceKeys": list(source_keys or []),
        "days": days,
    }


def build_citations(matches: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    citations = []
    for index, item in enumerate(matches, 1):
        citations.append(
            {
                "index": index,
                "id": item.get("id"),
                "title": item.get("title"),
                "url": item.get("url"),
                "source": item.get("source"),
                "publishedAt": item.get("publishedAt"),
                "snippet": item.get("snippet"),
                "score": item.get("score"),
            }
        )
    return citations
