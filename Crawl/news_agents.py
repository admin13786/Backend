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

# ====== LLM 六维度评分配置 ======
# LLM_SCORE_ENABLED: 是否启用 LLM 评分（"1"/"true" 开启，其他关闭）
LLM_SCORE_ENABLED = str(os.getenv("LLM_SCORE_ENABLED", "1")).strip().lower() not in {
    "", "0", "false", "off", "no",
}
# LLM_SCORE_WEIGHT: LLM 分在最终总分中的权重（0~1），剩余权重归 HN 分
LLM_SCORE_WEIGHT = float(os.getenv("LLM_SCORE_WEIGHT", "0.35"))
# LLM_SCORE_CONCURRENCY: 并发调用 LLM 评分的最大数量
LLM_SCORE_CONCURRENCY = int(os.getenv("LLM_SCORE_CONCURRENCY", "5"))
# LLM_SCORE_MODEL: 评分使用的模型
LLM_SCORE_MODEL = os.getenv("LLM_SCORE_MODEL", "qwen-plus").strip() or "qwen-plus"
LLM_AUDIENCE_MODEL = os.getenv("LLM_AUDIENCE_MODEL", LLM_SCORE_MODEL).strip() or LLM_SCORE_MODEL


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

    # ====== LLM 六维度评分 ======

    async def _score_article_llm(self, article: Dict) -> Dict[str, float]:
        """
        调用 LLM 对单篇文章进行六维度评分，返回 0-100 的分数。
        一次 LLM 调用同时输出六个维度，不拆成多次。

        输入上下文：标题 + 来源 + 发布时间 + 摘要 + 正文片段（1500字）
        + 热度五元组（heat_score / views / likes / comments / favorites）
        """
        if self.llm_client is None:
            self._warn_missing_llm_once("LLM scoring")
            return self._fallback_scores(article)

        title = str(article.get("title", "") or "").strip()
        summary = str(article.get("summary", "") or "")[:500].strip()
        content = str(article.get("content", "") or "")[:1500].strip()
        source = str(article.get("source", "") or "").strip()
        source_key = str(article.get("source_key", "") or "").strip()
        published_at = str(article.get("published_at", "") or "")[:19]

        heat_score = float(article.get("heat_score", 0) or 0)
        heat_views = int(article.get("heat_views", 0) or 0)
        heat_likes = int(article.get("heat_likes", 0) or 0)
        heat_comments = int(article.get("heat_comments", 0) or 0)
        heat_favorites = int(article.get("heat_favorites", 0) or 0)

        # 来源权威性提示
        high_authority = {"hacker_news", "openai_blog", "mit_tech_review",
                          "techcrunch_ai", "the_verge_ai", "ars_technica_ai"}
        authority_hint = "（高权威科技媒体/官方来源）" if source_key in high_authority else ""

        # 热度上下文：帮 LLM 理解数字含义
        if heat_score > 0 or heat_comments > 0:
            heat_context = (
                f"热度数据：综合热度={heat_score:.0f}，浏览量={heat_views}，"
                f"点赞={heat_likes}，评论={heat_comments}，收藏={heat_favorites}"
            )
        else:
            heat_context = "热度数据：（暂无，可能为新发布或数据源不提供热度）"

        prompt = f"""你是一位资深科技新闻编辑，请对下面这篇 AI/科技新闻从六个维度打分（每个维度 0-100 分）。

评分标准：
1. ai_relevance（AI 相关性）：这篇内容与人工智能/大模型/深度学习/智能体的相关程度。
   - 90-100: 核心讲 AI 技术/产品/研究
   - 70-89: 主要讲科技，AI 是重要部分
   - 40-69: 涉及科技但 AI 不是重点
   - 0-39: 基本与 AI 无关

2. industry_impact（行业影响力）：对科技行业/产业格局的影响程度。
   - 90-100: 可能改变行业格局（如 GPT-5 发布、千亿融资、重大监管政策）
   - 70-89: 对细分领域有较大影响（如某开源项目重大更新、重要论文发表）
   - 40-69: 有一定参考价值（产品更新、行业动态）
   - 0-39: 影响有限（八卦、软文、常规招聘/人事变动）

3. timeliness（时效性）：新闻的新鲜度和及时性。注意发布时间。
   - 90-100: 今日/昨日发生的突发事件或首次独家报道
   - 70-89: 近一周内事件的跟进报道或深度解读
   - 40-69: 有一定时效但非突发（如产品评测、行业分析）
   - 0-39: 教程/盘点/年度总结类，基本没有时效要求

4. content_quality（内容质量）：信息密度、论证严谨度、观点独特性。
   - 90-100: 信息密度极高，数据和引用扎实，观点有洞察力
   - 70-89: 内容充实，论证清晰，有一定深度
   - 40-69: 信息量适中，基本说清了事情
   - 0-39: 内容空洞、标题党、或大量充数段落

5. readability（可读性）：对非技术背景的普通读者的友好程度。
   - 90-100: 通俗易懂，专业术语有解释，行文流畅
   - 70-89: 基本易读，少量术语但不影响理解
   - 40-69: 需要一定背景知识才能读懂
   - 0-39: 高度学术化/充满未解释的技术黑话

6. spread_heat（传播热度）：综合热度数据和内容本身的传播潜力，在技术社区的讨论度。
   - {heat_context}
   - 90-100: 现象级传播，跨圈讨论
   - 70-89: 在技术社区引起广泛讨论
   - 40-69: 有一定关注度
   - 0-39: 尚无明显传播/冷门话题

只返回 JSON，不要 Markdown，不要解释：
{{"ai_relevance": 85, "industry_impact": 70, "timeliness": 90, "content_quality": 75, "readability": 60, "spread_heat": 80}}

新闻素材：
- 标题：{title}
- 来源：{source}{authority_hint}
- 发布时间：{published_at}
- 摘要：{summary}
- 正文片段：{content}
- {heat_context}"""

        try:
            response = await self.llm_client.chat.completions.create(
                model=LLM_SCORE_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            text = (response.choices[0].message.content or "").strip()

            # 解析 JSON：先找 ``` 代码块，再找裸 {}
            if "```" in text:
                for part in text.split("```"):
                    part = part.strip()
                    if part.startswith("json"):
                        part = part[4:].strip()
                    if part.startswith("{"):
                        text = part
                        break

            start = text.find("{")
            end = text.rfind("}")
            if start >= 0 and end > start:
                text = text[start:end + 1]

            scores = json.loads(text)
            return {
                "ai_relevance":    max(0.0, min(100.0, float(scores.get("ai_relevance", 0) or 0))),
                "industry_impact": max(0.0, min(100.0, float(scores.get("industry_impact", 0) or 0))),
                "timeliness":      max(0.0, min(100.0, float(scores.get("timeliness", 0) or 0))),
                "content_quality": max(0.0, min(100.0, float(scores.get("content_quality", 0) or 0))),
                "readability":     max(0.0, min(100.0, float(scores.get("readability", 0) or 0))),
                "spread_heat":     max(0.0, min(100.0, float(scores.get("spread_heat", 0) or 0))),
            }
        except Exception as exc:
            print(f"⚠️ LLM 六维度评分失败 [{str(article.get('title', '') or '')[:40]}]: {exc}")
            return self._fallback_scores(article)

    def _fallback_scores(self, article: Dict) -> Dict[str, float]:
        """
        LLM 不可用时的降级评分：用热度数据估算 spread_heat，
        用来源知名度估算 content_quality，其余给中性分。
        """
        heat = float(article.get("heat_score", 0) or 0)
        source = str(article.get("source", "") or "").lower()

        # 高权威来源列表
        high_authority_sources = {
            "openai", "mit technology review", "techcrunch",
            "ars technica", "the verge", "hacker news",
        }
        is_authority = any(s in source for s in high_authority_sources)

        # spread_heat: 按热度值简单分档
        if heat >= 500:
            spread = 90.0
        elif heat >= 200:
            spread = 75.0
        elif heat >= 50:
            spread = 55.0
        elif heat > 0:
            spread = 35.0
        else:
            spread = 15.0

        return {
            "ai_relevance": 70.0,                                    # 中性预设
            "industry_impact": 50.0,                                  # 中性预设
            "timeliness": 60.0,                                       # 中性预设
            "content_quality": 70.0 if is_authority else 50.0,       # 权威源偏高
            "readability": 60.0,                                      # 中性预设
            "spread_heat": spread,                                    # 热度分档
        }

    def _normalize_llm_scores(self, llm_scores: Dict[str, float]) -> float:
        """
        将 LLM 的 0-100 六维度得分归一化为 HN 分可比的单个分数。
        默认等权平均后除以 100 得到 0-1 的归一化值，
        再乘以参考量级（HN 分通常在 0~200 之间），映射到 HN 空间。
        """
        dims = ["ai_relevance", "industry_impact", "timeliness",
                "content_quality", "readability", "spread_heat"]
        avg = sum(llm_scores.get(d, 0) for d in dims) / len(dims)
        # 映射到 HN 分数量级：avg/100 * HN_SCALE
        HN_SCALE = 200.0
        return round(avg / 100.0 * HN_SCALE, 6)

    async def score_batch_llm(self, articles: List[Dict]) -> List[Dict]:
        """
        对一批文章并发调用 LLM 六维度评分，将结果写入每篇文章的字段。
        返回传入的 articles（原地修改）。
        """
        if not articles or not LLM_SCORE_ENABLED:
            return articles

        sem = asyncio.Semaphore(max(1, LLM_SCORE_CONCURRENCY))
        success_count = 0

        async def _one(article: Dict):
            nonlocal success_count
            async with sem:
                scores = await self._score_article_llm(article)
                article["ai_relevance"] = scores["ai_relevance"]
                article["industry_impact"] = scores["industry_impact"]
                article["timeliness"] = scores["timeliness"]
                article["content_quality"] = scores["content_quality"]
                article["readability"] = scores["readability"]
                article["spread_heat"] = scores["spread_heat"]
                # 归一化 LLM 分
                article["llm_score"] = self._normalize_llm_scores(scores)
                success_count += 1

        await asyncio.gather(
            *[_one(a) for a in articles], return_exceptions=True
        )
        print(f"🤖 LLM 六维度评分完成: {success_count}/{len(articles)} 篇")
        return articles

    # ====== LLM 六维度评分 END ======

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
        source_key = str(article.get("source_key", "") or "").lower()

        strong_non_news_keywords = [
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
            "collection",
            "curated",
            "from scratch",
            "benchmark repo",
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
        if any(keyword in text for keyword in strong_non_news_keywords):
            return False

        if source_key == "hacker_news" or "hacker news" in source:
            job_hints = [" is hiring", " jobs/", "/jobs/", "job?id=", "招聘"]
            if any(keyword in text for keyword in job_hints):
                return False
            return True

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
                model=LLM_AUDIENCE_MODEL,
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

        # ---- 第1遍：AI相关性过滤 + 英文翻译 ----
        valid_articles: List[Dict] = []
        for article in articles:
            is_digest = article.get("source_key") == "ai_digest"
            if not is_digest and not self._is_ai_related(article):
                filtered_out += 1
                continue

            title = str(article.get("title", "") or "")
            if title and not self._has_chinese(title):
                article["original_title"] = title
                article["title"] = await self._translate_title_to_zh(title)

            valid_articles.append(article)

        if filtered_out:
            print(f"🧹 筛选过滤 {filtered_out} 篇（AI 相关性）")

        # ---- 第2遍：LLM 六维度评分（批量并发） ----
        if LLM_SCORE_ENABLED and self.llm_client is not None:
            await self.score_batch_llm(valid_articles)
        else:
            if LLM_SCORE_ENABLED:
                self._warn_missing_llm_once("LLM scoring (disabled: no API key)")
            # LLM不可用时用降级分数
            for article in valid_articles:
                fb = self._fallback_scores(article)
                article["ai_relevance"] = fb["ai_relevance"]
                article["industry_impact"] = fb["industry_impact"]
                article["timeliness"] = fb["timeliness"]
                article["content_quality"] = fb["content_quality"]
                article["readability"] = fb["readability"]
                article["spread_heat"] = fb["spread_heat"]
                article["llm_score"] = self._normalize_llm_scores(fb)

        # ---- 第3遍：HN 排名分 + LLM 分加权合成 + 摘要 ----
        hh_weight = 1.0 - LLM_SCORE_WEIGHT   # HN 分权重
        ll_weight = LLM_SCORE_WEIGHT          # LLM 分权重

        for index, article in enumerate(valid_articles, 1):
            raw_hn = hacker_news_rank_score(article)
            llm_score = float(article.get("llm_score", 0) or 0)

            # 加权合成最终得分
            combined = raw_hn * hh_weight + llm_score * ll_weight

            print(
                f"📳 [{index}/{len(valid_articles)}] "
                f"HN={raw_hn:.3f} LLM={llm_score:.1f} "
                f"→ TOTAL={combined:.3f} (w_HN={hh_weight:.2f} w_LLM={ll_weight:.2f}) "
                f": {str(article.get('title', '') or '')[:50]}..."
            )

            try:
                article["total_score"] = round(combined, 6)
                article["hn_score_raw"] = round(raw_hn, 6)

                if not article.get("summary") or article["summary"] == article["title"]:
                    article["summary"] = await self._generate_summary(article)

                scored_articles.append(article)
            except Exception as exc:
                print(f"❌ 处理失败: {exc}")
                continue

        # ---- 排序：AI资讯速览置顶 + 其余按总分降序 ----
        digest_scored = [a for a in scored_articles if a.get("source_key") == "ai_digest"]
        other_scored  = [a for a in scored_articles if a.get("source_key") != "ai_digest"]

        max_other = 0.0
        if other_scored:
            max_other = max(float(a.get("total_score", 0) or 0) for a in other_scored)

        digest_scored.sort(key=lambda a: int(a.get("ai_digest_feed_order", 10**9)))
        floor = max_other + 1.0
        for idx, a in enumerate(digest_scored):
            a["total_score"] = round(floor + 1000.0 - idx * 0.0001, 6)

        other_scored.sort(key=lambda a: float(a.get("total_score", 0) or 0), reverse=True)
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
