"""
AI News Agent - 去重、Hacker News 风格排序、LLM 分流、标题翻译与摘要生成。
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Dict, List

from env_loader import load_crawl_env

load_crawl_env()

logger = logging.getLogger("news_agent")
HN_GRAVITY = float(os.getenv("HN_GRAVITY", "1.8"))


def _parse_published_utc(article: Dict) -> datetime:
    raw = article.get("published_at") or article.get("crawled_at")
    if not raw:
        return datetime.now(timezone.utc)
    if isinstance(raw, datetime):
        dt = raw
    else:
        s = str(raw).strip()
        try:
            if s.endswith("Z"):
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            elif "T" in s:
                dt = datetime.fromisoformat(s)
            else:
                dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _article_heat(article: Dict) -> float:
    return float(article.get("heat_score", 0) or article.get("spread_heat", 0) or 0)


def hacker_news_P_T(article: Dict, *, now: datetime | None = None) -> tuple[float, float]:
    now = now or datetime.now(timezone.utc)
    pub = _parse_published_utc(article)
    hours = (now - pub).total_seconds() / 3600.0
    t_hours = max(hours, 0.0)
    heat = _article_heat(article)
    p_score = max(heat + 1.0, 1.0)
    return p_score, t_hours


def hacker_news_rank_score(article: Dict, *, now: datetime | None = None) -> float:
    now = now or datetime.now(timezone.utc)
    p_score, t_hours = hacker_news_P_T(article, now=now)
    denominator = (t_hours + 2.0) ** HN_GRAVITY
    if denominator <= 0:
        return 0.0
    return (p_score - 1.0) / denominator


class NewsEditorAgent:
    def __init__(self, llm_client):
        self.llm_client = llm_client
        self._warned_missing_llm_features: set[str] = set()

    def _warn_missing_llm_once(self, feature: str) -> None:
        if self.llm_client is not None or feature in self._warned_missing_llm_features:
            return
        logger.warning(
            "DashScope API key is missing; %s is using fallback behavior.",
            feature,
        )
        self._warned_missing_llm_features.add(feature)

    def deduplicate(self, articles: List[Dict]) -> List[Dict]:
        if len(articles) <= 1:
            return articles

        unique_articles: List[Dict] = []
        seen_titles: List[str] = []
        for article in articles:
            title = str(article.get("title", "")).lower()
            is_duplicate = False
            for seen_title in seen_titles:
                if self._title_similarity(title, seen_title) > 0.6:
                    is_duplicate = True
                    break
            if not is_duplicate:
                unique_articles.append(article)
                seen_titles.append(title)

        print(f"✅ 去重完成: {len(articles)} -> {len(unique_articles)} 篇")
        return unique_articles

    def _title_similarity(self, title1: str, title2: str) -> float:
        words1 = set(title1.split())
        words2 = set(title2.split())
        if not words1 or not words2:
            return 0.0
        return len(words1.intersection(words2)) / len(words1.union(words2))

    def _has_chinese(self, text: str) -> bool:
        return bool(re.search(r"[\u4e00-\u9fff]", text or ""))

    def _fallback_summary(self, article: Dict) -> str:
        summary = str(article.get("summary", "") or "").strip()
        title = str(article.get("title", "") or "").strip()
        if summary and summary != title:
            return summary[:200]
        content = re.sub(r"\s+", " ", str(article.get("content", "") or "")).strip()
        if content:
            return content[:120]
        return title[:120]

    def _is_ai_related(self, article: Dict) -> bool:
        text = " ".join(
            [
                str(article.get("title", "") or ""),
                str(article.get("summary", "") or ""),
                str(article.get("content", "") or ""),
                str(article.get("source", "") or ""),
            ]
        ).lower()

        word_boundary_keywords = ["ai", "agi", "llm", "gpt", "rag", "npu"]
        substring_keywords = [
            "aigc",
            "chatgpt",
            "claude",
            "gemini",
            "deepseek",
            "copilot",
            "agent",
            "prompt",
            "embedding",
            "transformer",
            "diffusion",
            "stable diffusion",
            "sdxl",
            "lora",
            "pytorch",
            "tensorflow",
            "langchain",
            "openai",
            "anthropic",
            "machine learning",
            "deep learning",
            "neural network",
            "artificial intelligence",
            "generative",
            "large language model",
            "foundation model",
            "chatbot",
            "natural language processing",
            "computer vision",
            "robotics",
            "autonomous",
            "人工智能",
            "大模型",
            "机器学习",
            "深度学习",
            "神经网络",
            "智能体",
            "多模态",
            "生成式",
            "向量数据库",
            "提示词",
            "推理模型",
            "模型训练",
            "模型推理",
            "算力",
            "芯片",
            "cuda",
        ]

        for keyword in word_boundary_keywords:
            if re.search(rf"\b{keyword}\b", text):
                return True
        return any(keyword in text for keyword in substring_keywords)

    def _is_news_like(self, article: Dict) -> bool:
        title = str(article.get("title", "") or "").lower()
        content = str(article.get("content", "") or "").lower()
        text = f"{title} {content}"
        source = str(article.get("source", "") or "").lower()

        non_news_keywords = [
            "cheat sheet",
            "awesome",
            "roadmap",
            "tutorial",
            "course",
            "lesson",
            "quickstart",
            "readme",
            "boilerplate",
            "template",
            "toolkit",
            "collection",
            "curated",
            "from scratch",
            "benchmark repo",
            "github.com/",
            "速查表",
            "教程",
            "课程",
            "入门",
            "合集",
            "模板",
            "样例",
            "实战项目",
            "学习路线",
            "从零开始",
            "开源仓库",
            "仓库地址",
        ]
        if any(keyword in text for keyword in non_news_keywords):
            return False

        news_keywords = [
            "announced",
            "launch",
            "released",
            "raises",
            "funding",
            "acquires",
            "report",
            "breaking",
            "update",
            "security advisory",
            "vulnerability",
            "发布",
            "宣布",
            "上线",
            "融资",
            "收购",
            "报告",
            "通报",
            "漏洞",
            "更新",
            "官宣",
            "首发",
            "开源",
            "测评",
            "发布会",
            "财报",
        ]
        if any(keyword in text for keyword in news_keywords):
            return True

        if "hacker news" in source:
            event_hints = ["show hn", "launch", "released", "announced", "发布", "宣布", "上线"]
            return any(keyword in title for keyword in event_hints)

        return True

    async def _classify_audience(self, article: Dict) -> str:
        if self.llm_client is None:
            self._warn_missing_llm_once("audience classification")
            return "business"

        body = f"""标题: {article.get('title', '')}
摘要: {(article.get('summary', '') or '')[:400]}
来源: {article.get('source', '')}
正文片段: {(article.get('content', '') or '')[:900]}"""

        prompt = f"""你是一位科技资讯编辑。请判断这条内容更适合放在 personal 还是 business 榜单。

personal 适合：
- 开发者工具、框架、SDK、API、模型能力更新
- 开源项目、GitHub 趋势、Show HN
- 编程实践、工程方案、论文/基准/技术研究

business 适合：
- 公司融资、并购、财报、商业化定价
- 政策法规、版权、监管、行业趋势
- 职场变化、市场分析、企业战略

如果拿不准，默认返回 business。

只返回 JSON：
{{"audience": "business"}}
或
{{"audience": "personal"}}

内容：
{body}"""

        try:
            response = await self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
            text = (response.choices[0].message.content or "").strip()
            if "```" in text:
                for part in text.split("```"):
                    part = part.strip()
                    if part.startswith("json"):
                        part = part[4:].strip()
                    if part.startswith("{"):
                        text = part
                        break
            data = json.loads(text)
            audience = str(data.get("audience", "")).strip().lower()
            return "personal" if audience == "personal" else "business"
        except Exception as exc:
            print(f"⚠️ 受众分流 LLM 失败，默认 business: {exc}")
            return "business"

    async def split_by_audience_llm(self, articles: List[Dict]) -> Dict[str, List[Dict]]:
        if self.llm_client is None:
            self._warn_missing_llm_once("audience split")
            return {"business": list(articles), "personal": []}

        business: List[Dict] = []
        personal: List[Dict] = []
        concurrency = int(os.getenv("AUDIENCE_CLASSIFY_CONCURRENCY", "5"))
        sem = asyncio.Semaphore(max(1, concurrency))

        async def one(article: Dict):
            async with sem:
                category = await self._classify_audience(article)
                return article, category

        results = await asyncio.gather(*[one(article) for article in articles], return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                print(f"⚠️ 分流任务异常: {result}")
                continue
            article, category = result
            if category == "personal":
                personal.append(article)
            else:
                business.append(article)

        return {"business": business, "personal": personal}

    async def score_and_summarize(self, articles: List[Dict]) -> List[Dict]:
        scored_articles: List[Dict] = []
        filtered_out = 0

        for index, article in enumerate(articles, 1):
            is_digest = article.get("source_key") == "ai_digest"
            if not is_digest and not self._is_ai_related(article):
                filtered_out += 1
                continue

            title = str(article.get("title", "") or "")
            if title and not self._has_chinese(title):
                article["original_title"] = title
                article["title"] = await self._translate_title_to_zh(title)

            raw_hn = hacker_news_rank_score(article)
            print(
                f"📳 [{index}/{len(articles)}] HN分 {raw_hn:.6f} (G={HN_GRAVITY}) "
                f"P≈{max(float(article.get('heat_score', 0) or 0) + 1.0, 1.0):.1f} "
                f": {str(article.get('title', '') or '')[:50]}..."
            )

            try:
                article["total_score"] = round(raw_hn, 6)
                article["ai_relevance"] = 0.0
                article["industry_impact"] = 0.0
                article["timeliness"] = 0.0
                article["content_quality"] = 0.0
                article["readability"] = 0.0
                article["spread_heat"] = float(article.get("heat_score", 0) or 0)

                if not article.get("summary") or article["summary"] == article["title"]:
                    article["summary"] = await self._generate_summary(article)

                scored_articles.append(article)
            except Exception as exc:
                print(f"❌ 处理失败: {exc}")
                continue

        if filtered_out:
            print(f"🧹 筛选过滤 {filtered_out} 篇（AI 相关性）")

        digest_scored = [article for article in scored_articles if article.get("source_key") == "ai_digest"]
        other_scored = [article for article in scored_articles if article.get("source_key") != "ai_digest"]

        max_other = 0.0
        if other_scored:
            max_other = max(float(article.get("total_score", 0) or 0) for article in other_scored)

        digest_scored.sort(key=lambda article: int(article.get("ai_digest_feed_order", 10**9)))
        floor = max_other + 1.0
        for index, article in enumerate(digest_scored):
            article["total_score"] = round(floor + 1000.0 - index * 0.0001, 6)

        other_scored.sort(key=lambda article: float(article.get("total_score", 0) or 0), reverse=True)
        return digest_scored + other_scored

    async def _translate_title_to_zh(self, title: str) -> str:
        if not title or self.llm_client is None:
            if title:
                self._warn_missing_llm_once("title translation")
            return title

        try:
            prompt = f"""请把下面这条新闻标题翻译成简体中文，要求：
1. 保留 GPT、CUDA、PyTorch、公司名等术语的准确性；
2. 风格像中文科技媒体标题；
3. 尽量控制在 35 个汉字内；
4. 只返回翻译后的标题，不要解释。

原标题：{title}"""

            response = await self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            translated = (response.choices[0].message.content or "").strip().strip('"').strip("'").strip()
            return translated or title
        except Exception as exc:
            print(f"❌ 标题翻译失败: {exc}")
            return title

    async def _generate_summary(self, article: Dict) -> str:
        if self.llm_client is None:
            self._warn_missing_llm_once("summary generation")
            return self._fallback_summary(article)

        try:
            prompt = f"""请为以下新闻生成一句话摘要（30-50字），概括核心事件和影响。

标题: {article['title']}
内容: {str(article.get('content', '') or '')[:1000]}

只返回摘要文本，不要其他内容。"""

            response = await self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
            )
            summary = (response.choices[0].message.content or "").strip().strip('"').strip("'").strip()
            return summary[:200]
        except Exception as exc:
            print(f"❌ 摘要生成失败: {exc}")
            return self._fallback_summary(article)
