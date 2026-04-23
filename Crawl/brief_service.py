"""
在爬虫入库前为新闻生成 brief_json。
- ai_digest 文章优先直接解析 RSS 正文
- 其他来源优先走 LLM，没有可用 key 时降级为本地拼装
"""

import asyncio
import json
import logging
import re
from typing import Dict, List
from urllib.parse import urlparse

logger = logging.getLogger("brief_service")
_SOURCE_RE = re.compile(r"(?:Sources?\s*[:：]|来源\s*[:：])", re.IGNORECASE)
_WARNED_MISSING_LLM = False


def _clamp_text(text: str, limit: int) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else text[:limit].rstrip() + "…"


def _extract_domain(url: str) -> str:
    try:
        hostname = urlparse(url).hostname or ""
    except Exception:
        return ""
    return hostname.replace("www.", "")


def _warn_missing_llm_once() -> None:
    global _WARNED_MISSING_LLM
    if _WARNED_MISSING_LLM:
        return
    logger.warning(
        "DashScope API key is missing; brief generation will use local fallback content."
    )
    _WARNED_MISSING_LLM = True


def _split_long_paragraph(text: str, chunk_limit: int = 150) -> List[str]:
    sentences = re.split(r"(?<=[。！？!?])", text)
    chunks: List[str] = []
    current = ""
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        if len(current) + len(sentence) > chunk_limit and current:
            chunks.append(current.strip())
            current = sentence
        else:
            current += sentence
    if current.strip():
        chunks.append(current.strip())
    return chunks


def _parse_digest_content(content: str) -> Dict:
    text = (content or "").strip()
    body = text
    takeaways: List[str] = []
    source_titles: List[str] = []
    raw_sources = ""

    why_parts = re.split(r"为什么重要\s*[:：]", text, maxsplit=1)
    if len(why_parts) == 2:
        body = why_parts[0].strip()
        rest = why_parts[1].strip()
        source_parts = _SOURCE_RE.split(rest, maxsplit=1)
        if len(source_parts) == 2:
            raw_tags = source_parts[0].strip()
            raw_sources = source_parts[1].strip()
        else:
            raw_tags = rest
        takeaways = [item.strip() for item in re.split(r"[；;]", raw_tags) if item.strip()]
    else:
        source_parts = _SOURCE_RE.split(text, maxsplit=1)
        if len(source_parts) == 2:
            body = source_parts[0].strip()
            raw_sources = source_parts[1].strip()

    body = re.sub(r"[。\.]\s*来源\s*$", "。", body).strip()
    body = re.sub(r"\s*来源\s*$", "", body).strip()

    if raw_sources:
        cutoff = re.search(r"\s+\d+\.\s", raw_sources)
        if cutoff:
            raw_sources = raw_sources[: cutoff.start()]
        source_titles = [item.strip() for item in raw_sources.split(",") if item.strip()]

    raw_paragraphs = [item.strip() for item in body.split("\n") if item.strip()]
    paragraphs: List[str] = []
    for paragraph in raw_paragraphs:
        if len(paragraph) <= 200:
            paragraphs.append(paragraph)
        else:
            paragraphs.extend(_split_long_paragraph(paragraph))
    if not paragraphs and body:
        paragraphs = [body]

    return {
        "paragraphs": paragraphs,
        "takeaways": takeaways,
        "source_titles": source_titles,
    }


def _build_brief_from_digest(article: Dict) -> Dict:
    title = str(article.get("title", "") or "").strip()
    url = str(article.get("url", "") or "").strip()
    content = str(article.get("content", "") or "").strip()
    summary = str(article.get("summary", "") or "").strip()
    domain = _extract_domain(url)
    parsed = _parse_digest_content(content)

    paragraphs = parsed["paragraphs"]
    if paragraphs:
        first = re.sub(r"^\d+\.\s*", "", paragraphs[0]).strip()
        if title and first.startswith(title):
            first = first[len(title):].strip()
        lead = first or title
        body_paragraphs = [re.sub(r"^\d+\.\s*", "", item).strip() for item in paragraphs if item.strip()]
    else:
        lead = title
        body_paragraphs = []

    if len(body_paragraphs) <= 1 and summary:
        summary_clean = re.sub(r"\s*来源\s*$", "", summary).strip()
        already_present = any(
            summary_clean == item or item.startswith(summary_clean) or summary_clean.startswith(item)
            for item in body_paragraphs
        )
        if summary_clean and summary_clean != lead and not already_present:
            body_paragraphs.append(summary_clean)

    tags = parsed["takeaways"] if parsed["takeaways"] else ["AI", "资讯"]
    sources = [
        {"label": title_item, "url": url, "domain": domain}
        for title_item in parsed["source_titles"]
    ]
    if not sources and url:
        sources.append({"label": title or "来源", "url": url, "domain": domain})

    return {
        "style": "ai_digest",
        "headline": title,
        "lead": _clamp_text(lead, 300),
        "paragraphs": body_paragraphs,
        "tags": tags,
        "sources": sources,
    }


def _build_fallback_brief(article: Dict) -> Dict:
    title = str(article.get("title", "") or "").strip()
    summary = _clamp_text(str(article.get("summary", "") or ""), 500)
    content = re.sub(r"\s+", " ", str(article.get("content", "") or "")).strip()
    content = _clamp_text(content, 1800)
    source = str(article.get("source", "") or "").strip()
    url = str(article.get("url", "") or "").strip()

    paragraphs: List[str] = []
    if summary:
        paragraphs.append(summary)
    if content:
        sentence_paragraphs = [
            item.strip()
            for item in re.split(r"(?<=[。！？.!?])\s*", content[:360])
            if item.strip()
        ]
        if len(sentence_paragraphs) >= 2:
            paragraphs.extend(sentence_paragraphs[:3])
        elif len(content) > 180:
            paragraphs.extend(_split_long_paragraph(content[:360], chunk_limit=150))
        else:
            paragraphs.append(content)

    deduped: List[str] = []
    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if paragraph and paragraph not in deduped:
            deduped.append(paragraph)

    if not deduped:
        deduped = [title] if title else ["该条新闻暂无可用摘要"]

    lead = deduped[0] if deduped else title
    return {
        "style": "fallback",
        "headline": title,
        "lead": lead or title or "该条新闻暂无可用摘要",
        "paragraphs": deduped[:4],
        "tags": ["AI", "资讯", "速览"],
        "sources": [{"label": source or "来源", "url": url}] if url else [],
    }


def _merge_with_fallback(primary: Dict, fallback: Dict) -> Dict:
    primary_paragraphs = [
        str(item or "").strip()
        for item in (primary.get("paragraphs") or [])
        if str(item or "").strip()
    ]
    fallback_paragraphs = [
        str(item or "").strip()
        for item in (fallback.get("paragraphs") or [])
        if str(item or "").strip()
    ]
    merged_paragraphs: List[str] = []
    for paragraph in primary_paragraphs + fallback_paragraphs:
        if paragraph and paragraph not in merged_paragraphs:
            merged_paragraphs.append(paragraph)

    result = dict(fallback)
    result.update(
        {
            "style": str(primary.get("style") or fallback.get("style") or "fallback"),
            "headline": str(primary.get("headline") or fallback.get("headline") or ""),
            "lead": str(primary.get("lead") or fallback.get("lead") or ""),
            "paragraphs": merged_paragraphs[:5],
            "tags": primary.get("tags") or fallback.get("tags") or [],
            "sources": primary.get("sources") or fallback.get("sources") or [],
        }
    )
    return result


async def _build_brief_via_llm(article: Dict, llm_client) -> Dict:
    if llm_client is None:
        _warn_missing_llm_once()
        return _build_fallback_brief(article)

    title = str(article.get("title", "") or "").strip()
    source = str(article.get("source", "") or "").strip()
    url = str(article.get("url", "") or "").strip()
    summary = _clamp_text(str(article.get("summary", "") or ""), 500)
    content = _clamp_text(str(article.get("content", "") or ""), 1800)

    prompt = f"""你是一位中文科技媒体编辑。请把一条新闻改写成「AI资讯速览」风格，要求：
1. 语言简洁，信息密度高；
2. 不要编造事实，未确认信息用“据称/报道称/消息称”等保守表述；
3. 只输出 JSON，不要 Markdown；
4. paragraphs 每段 1-2 句，总共 4-7 段；
5. tags 返回 3-6 个短标签。

返回 JSON Schema：
{{
  "style": "generated",
  "headline": string,
  "lead": string,
  "paragraphs": string[],
  "tags": string[],
  "sources": [{{"label": string, "url": string}}]
}}

输入素材：
- 标题：{title}
- 来源：{source}
- 原文链接：{url}
- 一句话摘要：{summary}
- 正文片段：{content}
"""

    try:
        response = await llm_client.chat.completions.create(
            model="qwen-plus",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
        )
        text = str(response.choices[0].message.content or "").strip()
        if "```" in text:
            for part in text.split("```"):
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{") and part.endswith("}"):
                    text = part
                    break
        brief = json.loads(text)
        if isinstance(brief, dict):
            brief.setdefault("style", "generated")
            brief.setdefault("headline", title)
            if url:
                sources = brief.get("sources")
                if not isinstance(sources, list) or not sources:
                    brief["sources"] = [{"label": source or "来源", "url": url}]
            return brief
    except Exception as exc:
        logger.warning("Brief LLM generation failed [%s]: %s", title[:40], exc)
    return _build_fallback_brief(article)


def _brief_has_enough_content(brief: Dict) -> bool:
    paragraphs = brief.get("paragraphs", [])
    return (
        isinstance(paragraphs, list)
        and len(paragraphs) >= 2
        and any(len(str(item)) > 30 for item in paragraphs)
    )


def generate_brief_for_article_sync(article: Dict) -> str:
    brief = _build_brief_from_digest(article)
    return json.dumps(brief, ensure_ascii=False)


async def generate_brief_for_article(article: Dict, llm_client) -> str:
    source_key = str(article.get("source_key", "") or "").strip()
    fallback = _build_fallback_brief(article)

    if source_key == "ai_digest":
        digest_brief = _build_brief_from_digest(article)
        if _brief_has_enough_content(digest_brief):
            return json.dumps(digest_brief, ensure_ascii=False)
        if llm_client is None:
            _warn_missing_llm_once()
            return json.dumps(_merge_with_fallback(digest_brief, fallback), ensure_ascii=False)
        logger.info("ai_digest content is insufficient, falling back to LLM: %s", str(article.get("title", ""))[:40])

    brief = await _build_brief_via_llm(article, llm_client)
    return json.dumps(brief, ensure_ascii=False)


async def enrich_articles_with_briefs(
    articles: List[Dict],
    llm_client,
    concurrency: int = 5,
) -> List[Dict]:
    if not articles:
        return articles

    sem = asyncio.Semaphore(max(1, concurrency))

    async def _worker(article: Dict) -> None:
        if article.get("brief_json"):
            return

        source_key = str(article.get("source_key", "") or "").strip()
        if source_key == "ai_digest":
            brief = _build_brief_from_digest(article)
            if _brief_has_enough_content(brief):
                article["brief_json"] = json.dumps(brief, ensure_ascii=False)
                return

        async with sem:
            article["brief_json"] = await generate_brief_for_article(article, llm_client)

    await asyncio.gather(*[_worker(article) for article in articles], return_exceptions=True)
    success = sum(1 for article in articles if article.get("brief_json"))
    print(f"📝 概述生成完成: {success}/{len(articles)}")
    return articles
