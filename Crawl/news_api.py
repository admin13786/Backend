"""
AI News API - RSS 新闻采集和评分系统
RSS/API 采集 → 评分 Agent → 数据库 → 前端
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import List
from openai import AsyncOpenAI
import logging
import os

from env_loader import get_dashscope_api_key, load_crawl_env
from news_agents import NewsEditorAgent
from tz_display import batch_ts_suffix
from rss_collector import collect_from_rss
from db import get_article_by_id
from db import get_latest_push_brief_item_by_article_id
from db import upsert_articles
from cover_service import enrich_articles_with_ai_covers, enrich_articles_with_covers
from brief_service import enrich_articles_with_briefs

# 创建路由
news_router = APIRouter(prefix="/news", tags=["AI News"])
logger = logging.getLogger("news_api")

load_crawl_env()

# 阿里百炼 API 配置
DASHSCOPE_API_KEY = get_dashscope_api_key()
DASHSCOPE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
_WARNED_LLM_DISABLED = False

# OpenAI 兼容客户端（用于评分和摘要）
llm_client = (
    AsyncOpenAI(
        api_key=DASHSCOPE_API_KEY,
        base_url=DASHSCOPE_BASE_URL,
    )
    if DASHSCOPE_API_KEY
    else None
)
editor_agent = NewsEditorAgent(llm_client)


def _warn_llm_disabled_once() -> None:
    global _WARNED_LLM_DISABLED
    if _WARNED_LLM_DISABLED or llm_client is not None:
        return
    logger.warning(
        "DashScope API key is missing; crawler LLM stages will use fallbacks "
        "(audience split, title translation, summary rewrite, brief generation)."
    )
    _WARNED_LLM_DISABLED = True


class NewsArticle(BaseModel):
    """新闻文章"""
    title: str
    url: str
    summary: str
    source: str
    total_score: float
    ai_relevance: float = 0
    industry_impact: float = 0
    spread_heat: float = 0
    timeliness: float = 0
    content_quality: float = 0
    readability: float = 0
    published_at: str


class NewsBriefRequest(BaseModel):
    """从榜单/文章生成「速览风格」内容"""
    news_id: int


def _clamp_text(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[:n].rstrip() + "…"


def _normalize_brief_paragraphs(value) -> List[str]:
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    text = str(value or "").strip()
    return [text] if text else []


def _build_selected_issue_brief(item: dict, article: dict) -> dict:
    brief_meta = item.get("brief") if isinstance(item.get("brief"), dict) else {}
    headline = str(item.get("headline") or article.get("title") or "").strip()
    lead = str(item.get("warning") or "").strip()
    paragraphs = _normalize_brief_paragraphs(item.get("expandedBody"))
    article_url = str(item.get("articleUrl") or article.get("url") or "").strip()
    source = str(item.get("source") or article.get("source") or "").strip()

    if not lead:
        lead = _clamp_text(str(article.get("summary", "") or ""), 140)
    if not paragraphs:
        fallback_paragraphs = [
            str(paragraph or "").strip()
            for paragraph in (brief_meta.get("paragraphs") or [])
            if str(paragraph or "").strip()
        ]
        paragraphs = fallback_paragraphs or ([lead] if lead else [headline])

    tags = brief_meta.get("tags") if isinstance(brief_meta.get("tags"), list) else []
    normalized_tags = [str(tag or "").strip() for tag in tags if str(tag or "").strip()]
    if not normalized_tags:
        normalized_tags = ["AI趣闻", "今日热榜", "易懂版"]

    sources = brief_meta.get("sources") if isinstance(brief_meta.get("sources"), list) else []
    normalized_sources = []
    for source_item in sources:
        if not isinstance(source_item, dict):
            continue
        label = str(source_item.get("label") or "").strip()
        url = str(source_item.get("url") or "").strip()
        if label or url:
            normalized_sources.append({"label": label or source or "原文", "url": url or article_url})
    if not normalized_sources and article_url:
        normalized_sources = [{"label": source or "原文", "url": article_url}]

    return {
        "style": str(brief_meta.get("style") or "selected_issue").strip() or "selected_issue",
        "headline": headline,
        "lead": lead or headline,
        "paragraphs": paragraphs,
        "tags": normalized_tags,
        "sources": normalized_sources,
        "issueId": str(item.get("issueId") or "").strip(),
        "issueDate": str(item.get("issueDate") or "").strip(),
    }


@news_router.get("/{news_id}")
async def get_news_by_id(news_id: int):
    """按 id 获取单条新闻（用于前端详情/速览页）。"""
    article = await get_article_by_id(news_id)
    if not article:
        return {"success": False, "message": f"未找到新闻：{news_id}"}
    return {"success": True, "data": article}


@news_router.post("/brief")
async def generate_news_brief(req: NewsBriefRequest):
    """
    返回「AI资讯速览」同风格的速览正文。
    优先读取爬虫阶段预生成的 brief_json；若库中无预生成数据则实时兜底生成。
    """
    article = await get_article_by_id(req.news_id)
    if not article:
        return {"success": False, "message": f"未找到新闻：{req.news_id}"}

    import json as _json

    selected_issue_item = await get_latest_push_brief_item_by_article_id(req.news_id)
    if selected_issue_item:
        selected_brief = _build_selected_issue_brief(selected_issue_item, article)
        paragraphs = selected_brief.get("paragraphs") if isinstance(selected_brief, dict) else None
        if (
            isinstance(selected_brief, dict)
            and selected_brief.get("headline")
            and isinstance(paragraphs, list)
            and len(paragraphs) >= 1
        ):
            return {"success": True, "data": selected_brief}

    # 优先使用爬虫流程中预生成的概述
    stored_brief = str(article.get("brief_json", "") or "").strip()
    if stored_brief:
        try:
            brief = _json.loads(stored_brief)
            paras = brief.get("paragraphs") if isinstance(brief, dict) else None
            has_enough = (
                isinstance(paras, list)
                and len(paras) >= 2
                and any(len(str(p)) > 20 for p in paras)
            )
            if isinstance(brief, dict) and brief.get("headline") and has_enough:
                return {"success": True, "data": brief}
        except (ValueError, TypeError):
            pass

    # 预生成数据不可用或内容不足，实时 LLM 生成
    from brief_service import generate_brief_for_article
    try:
        brief_str = await generate_brief_for_article(article, llm_client)
        brief = _json.loads(brief_str)
        return {"success": True, "data": brief}
    except Exception as e:
        title = str(article.get("title", "") or "").strip()
        summary = _clamp_text(str(article.get("summary", "") or ""), 500)
        content = _clamp_text(str(article.get("content", "") or ""), 1800)
        source = str(article.get("source", "") or "").strip()
        url = str(article.get("url", "") or "").strip()
        fallback = {
            "style": "fallback",
            "headline": title,
            "lead": summary or _clamp_text(content, 140) or "该条新闻暂无可用摘要",
            "paragraphs": [p for p in [summary, content] if p] or [title],
            "tags": ["AI", "资讯", "速览"],
            "sources": [{"label": source or "来源", "url": url}] if url else [],
            "error": str(e),
        }
        return {"success": True, "data": fallback}

@news_router.get("/health")
async def health_check():
    """健康检查"""
    return {
        "status": "ok",
        "service": "AI News Agent (RSS模式)",
        "version": "3.0.0",
        "features": [
            "RSS 直连采集（AI资讯速览官方 feed，最高排序优先级）",
            "RSS 直连采集（Hacker News）",
            "API 兜底采集（GitHub/B站）",
            "热度 API 补齐（HN/GitHub/B站）",
            "Hacker News 重力衰减排序 (P-1)/(T+2)^G",
            "LLM 商业/个人受众分流",
            "一句话摘要生成",
            "双榜模式（商业/个人）",
        ]
    }


# ========== 定时任务函数 ==========

async def scheduled_crawl(mode: str):
    """
    定时爬取任务（RSS 模式）
    说明：统一爬取一次后，按内容分流到企业榜/个人榜两个批次
    """
    mode_name = "分流双榜"
    ts = batch_ts_suffix()
    biz_batch_id = f"biz_{ts}"
    personal_batch_id = f"personal_{ts}"

    print(f"\n{'='*50}")
    print(f"⏰ 定时任务启动: {mode_name}")
    print(f"📋 企业榜批次ID: {biz_batch_id}")
    print(f"📋 个人榜批次ID: {personal_batch_id}")
    print(f"📡 数据源: 直连RSS/API兜底 + 热度 API")
    print(f"{'='*50}")

    try:
        _warn_llm_disabled_once()
        # 1. 采集文章并补齐热度 API
        all_articles = await collect_from_rss()

        if not all_articles:
            print(f"⚠️ [{mode_name}] 未采集到任何新闻")
            return

        print(f"✅ [{mode_name}] RSS 采集 {len(all_articles)} 篇")

        # 2. 去重 + LLM 分流（组织/企业 vs 个人）
        unique = editor_agent.deduplicate(all_articles)
        split = await editor_agent.split_by_audience_llm(unique)
        biz_articles = split["business"]
        personal_articles = split["personal"]
        print(f"📌 LLM 分流：企业 {len(biz_articles)} 篇，个人 {len(personal_articles)} 篇")

        # 3. 企业榜：HN 排序分 + 摘要 + 封面 + 概述 + 存库
        biz_scored = []
        if biz_articles:
            biz_scored = await editor_agent.score_and_summarize(biz_articles)
            await enrich_articles_with_covers(biz_scored)
            await enrich_articles_with_ai_covers(biz_scored)
            await enrich_articles_with_briefs(biz_scored, llm_client)
            for a in biz_scored:
                a["crawl_batch"] = biz_batch_id
            await upsert_articles(biz_scored, batch_id=biz_batch_id)

        # 4. 个人榜：同上
        personal_scored = []
        if personal_articles:
            personal_scored = await editor_agent.score_and_summarize(personal_articles)
            await enrich_articles_with_covers(personal_scored)
            await enrich_articles_with_ai_covers(personal_scored)
            await enrich_articles_with_briefs(personal_scored, llm_client)
            for a in personal_scored:
                a["crawl_batch"] = personal_batch_id
            await upsert_articles(personal_scored, batch_id=personal_batch_id)

        print(
            f"✅ [{mode_name}] 完成！企业榜 {len(biz_scored)} 篇（{biz_batch_id}），"
            f"个人榜 {len(personal_scored)} 篇（{personal_batch_id}）"
        )

    except Exception as e:
        print(f"❌ [{mode_name}] 定时任务失败: {e}")
        import traceback
        traceback.print_exc()
