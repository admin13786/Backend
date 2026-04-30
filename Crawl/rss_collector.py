"""
RSS 新闻采集器（AI 热榜版）
从指定站点拉取 RSS，再补齐热度 API 字段。
- RSS: 直连官方/可用源，失败时走 API 兜底
- 热度 API: Hacker News / GitHub / Bilibili / Product Hunt(可选 token)
"""

import asyncio
import json
import logging
import math
import os
import re as _re
from collections import defaultdict
from datetime import datetime
from time import mktime

from tz_display import TZ_SHANGHAI
from typing import Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

import feedparser
from env_loader import load_crawl_env
from url_utils import normalize_article_url

load_crawl_env()

logger = logging.getLogger("rss_collector")

# AI 资讯速览（官方 RSS）：参与双榜分流，排序阶段给极高热度以保证榜单优先级（需在 TARGET_FEEDS 之前定义）
AI_DIGEST_RSS_URL = os.getenv(
    "AI_DIGEST_RSS_URL", "https://ai-digest.liziran.com/zh/feed.xml"
).strip()
AI_DIGEST_PRIORITY_HEAT = int(os.getenv("AI_DIGEST_PRIORITY_HEAT", "80000"))

# RSSHub 实例地址（可通过环境变量自定义，默认官方实例）
RSSHUB_BASE = os.getenv("RSSHUB_BASE", "https://rsshub.app").strip().rstrip("/")

# ========== RSS 数据源配置 ==========

# 新站点：仅直连 RSS + API 兜底；AI 资讯速览列于首位（官方中文策展信源）
TARGET_FEEDS = [
    {
        "source_key": "ai_digest",
        "name": "AI资讯速览",
        "rss_url": AI_DIGEST_RSS_URL,
        "fallback_url": AI_DIGEST_RSS_URL,
    },
    {
        "source_key": "hacker_news",
        "name": "Hacker News",
        "rss_url": "https://hnrss.org/frontpage",
        "fallback_url": "https://hnrss.org/frontpage",
    },
    {
        "source_key": "github",
        "name": "GitHub Trending AI",
        "rss_url": "",
        "fallback_url": "",
    },
    {
        "source_key": "bilibili",
        "name": "Bilibili 科技热榜",
        "rss_url": f"{RSSHUB_BASE}/bilibili/ranking/1012",
        "fallback_url": "",
    },
    {
        "source_key": "techcrunch_ai",
        "name": "TechCrunch AI",
        "rss_url": "https://techcrunch.com/category/artificial-intelligence/feed/",
        "fallback_url": "https://techcrunch.com/feed/",
    },
    {
        "source_key": "the_verge_ai",
        "name": "The Verge AI",
        "rss_url": "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml",
        "fallback_url": "https://www.theverge.com/rss/index.xml",
    },
    {
        "source_key": "mit_tech_review",
        "name": "MIT Technology Review",
        "rss_url": "https://www.technologyreview.com/feed/",
        "fallback_url": "https://www.technologyreview.com/feed/",
    },
    {
        "source_key": "ars_technica_ai",
        "name": "Ars Technica AI",
        "rss_url": "https://feeds.arstechnica.com/arstechnica/technology-lab",
        "fallback_url": "https://feeds.arstechnica.com/arstechnica/index",
    },
    {
        "source_key": "openai_blog",
        "name": "OpenAI Blog",
        "rss_url": "https://openai.com/blog/rss.xml",
        "fallback_url": "https://openai.com/blog/rss.xml",
        "max_articles": 100,
    },
]

# 请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}
PRODUCT_HUNT_TOKEN = os.getenv("PRODUCT_HUNT_TOKEN", "").strip()

# 入库前热度校准：消除 GitHub star 与 HN 赞数量级差（对数缩放 + 组内归一化）
# HEAT_CALIBRATION: logminmax（默认）| quantile | none
_DEFAULT_CAL_SRC = "github,hacker_news,bilibili"
HEAT_CALIBRATION = os.getenv("HEAT_CALIBRATION", "logminmax").strip().lower()
HEAT_CAL_MAX = float(os.getenv("HEAT_CAL_MAX", "400"))
HEAT_CALIBRATION_SOURCES = frozenset(
    x.strip()
    for x in os.getenv("HEAT_CALIBRATION_SOURCES", _DEFAULT_CAL_SRC).split(",")
    if x.strip()
)


def _calibrate_log_minmax(group: List[Dict], cal_max: float) -> None:
    """组内 log1p(raw) 后 min-max 映射到 [0, cal_max]；保留 heat_score_raw。"""
    raws = [max(0.0, float(a.get("heat_score", 0) or 0)) for a in group]
    n = len(group)
    if n == 0:
        return
    if max(raws) <= 0:
        for a, r in zip(group, raws):
            a["heat_score_raw"] = int(r)
        return
    logs = [math.log1p(r) for r in raws]
    lo, hi = min(logs), max(logs)
    span = (hi - lo) if hi > lo else 0.0
    for a, r, lg in zip(group, raws, logs):
        a["heat_score_raw"] = int(r)
        if r <= 0:
            a["heat_score"] = 0
            continue
        if span <= 0:
            new_h = int(round(cal_max * 0.85))
        else:
            norm = (lg - lo) / span
            new_h = int(round(norm * cal_max))
        a["heat_score"] = max(0, min(int(cal_max), new_h))


def _calibrate_quantile(group: List[Dict], cal_max: float) -> None:
    """组内按原始 heat 排序分位（并列取平均名次）映射到 [0, cal_max]。"""
    raws = [max(0.0, float(a.get("heat_score", 0) or 0)) for a in group]
    n = len(group)
    if n == 0:
        return
    if max(raws) <= 0:
        for a, r in zip(group, raws):
            a["heat_score_raw"] = int(r)
        return
    if n == 1:
        a = group[0]
        r = raws[0]
        a["heat_score_raw"] = int(r)
        a["heat_score"] = int(cal_max) if r > 0 else 0
        return

    sorted_idx = sorted(range(n), key=lambda i: raws[i])
    pctl = [0.0] * n
    i = 0
    denom = max(n - 1, 1)
    while i < n:
        j = i + 1
        while j < n and raws[sorted_idx[j]] == raws[sorted_idx[i]]:
            j += 1
        avg_pos = (i + j - 1) / 2.0
        pr = avg_pos / denom
        for k in range(i, j):
            pctl[sorted_idx[k]] = pr
        i = j

    for idx, a in enumerate(group):
        r = raws[idx]
        a["heat_score_raw"] = int(r)
        if r <= 0:
            a["heat_score"] = 0
        else:
            new_h = int(round(pctl[idx] * cal_max))
            a["heat_score"] = max(0, min(int(cal_max), new_h))


def apply_source_heat_calibration(articles: List[Dict]) -> None:
    """
    按 source_key 分组，仅对配置中的源做校准（默认 GitHub / HN / B站 / PH）。
    原始值写入 heat_score_raw，heat_score 为可比量级。
    """
    mode = HEAT_CALIBRATION
    if mode in ("", "none", "off", "false", "0", "no"):
        return
    groups: Dict[str, List[Dict]] = defaultdict(list)
    for a in articles:
        sk = (a.get("source_key") or "").strip()
        if sk in HEAT_CALIBRATION_SOURCES:
            groups[sk].append(a)
    cal_max = HEAT_CAL_MAX
    for sk, group in groups.items():
        if mode == "quantile":
            _calibrate_quantile(group, cal_max)
        else:
            _calibrate_log_minmax(group, cal_max)
    if groups:
        logger.info(
            "热度校准 mode=%s max=%s 源: %s",
            mode,
            cal_max,
            ",".join(sorted(groups.keys())),
        )


def _parse_published(entry) -> str:
    """从 RSS entry 中提取发布时间，返回 ISO 格式字符串"""
    for field in ("published_parsed", "updated_parsed"):
        parsed = getattr(entry, field, None)
        if parsed:
            try:
                return datetime.fromtimestamp(mktime(parsed)).isoformat()
            except Exception:
                pass
    for field in ("published", "updated"):
        val = getattr(entry, field, None)
        if val:
            return val
    return datetime.now().isoformat()


def _extract_content(entry) -> str:
    """从 RSS entry 中提取正文内容，优先全文，其次摘要"""
    # 尝试全文（content 字段）
    content_list = getattr(entry, "content", None)
    if content_list and isinstance(content_list, list):
        for c in content_list:
            val = c.get("value", "")
            if val and len(val) > 100:
                return _strip_html(val)

    # 尝试摘要（summary / description）
    summary = getattr(entry, "summary", "") or ""
    if summary:
        return _strip_html(summary)

    # 尝试 description
    desc = getattr(entry, "description", "") or ""
    if desc:
        return _strip_html(desc)

    return ""


def _strip_html(text: str) -> str:
    """简单去除 HTML 标签"""
    text = _re.sub(r'<[^>]+>', '', text)
    text = _re.sub(r'&[a-zA-Z]+;', ' ', text)
    text = _re.sub(r'\s+', ' ', text).strip()
    return text


def _extract_raw_html_entry(entry) -> str:
    """RSS 条目中的原始 HTML（用于解析 Sources 外链）"""
    content_list = getattr(entry, "content", None)
    if content_list and isinstance(content_list, list):
        for c in content_list:
            if not isinstance(c, dict):
                continue
            val = c.get("value", "") or ""
            if val and len(val) > 80:
                return val
    for attr in ("summary", "description"):
        val = getattr(entry, attr, None) or ""
        if val and ("href=" in val.lower() or "http" in val.lower()):
            return val
    return ""


def _is_digest_internal_href(href: str) -> bool:
    u = (href or "").strip().lower()
    if not u:
        return True
    if "ai-digest.liziran.com" in u:
        return True
    if "liziran.com" in u and "/digest/" in u:
        return True
    return False


def _split_digest_html_into_articles(raw_html: str, base_article: Dict) -> List[Dict]:
    """
    将 AI 资讯速览日刊的单条 RSS item（包含多篇新闻）拆分为独立文章列表。
    日刊结构：<h2>N. 主文标题</h2> ... <hr> ... <h2>快讯</h2> <p><strong>快讯标题</strong>...</p>
    """
    if not raw_html or not raw_html.strip():
        return [base_article]

    href_pat = _re.compile(r"""href\s*=\s*["'](https?://[^"'>\s]+)["']""", _re.I)
    h2_pat = _re.compile(r'<h2[^>]*>(.*?)</h2>', _re.I | _re.S)
    strong_pat = _re.compile(r'<strong[^>]*>(.*?)</strong>', _re.I | _re.S)

    h2_matches = list(h2_pat.finditer(raw_html))
    if not h2_matches:
        return [base_article]

    articles = []
    kuaixun_start = None

    for idx, m in enumerate(h2_matches):
        h2_text = _strip_html(m.group(1)).strip()
        section_start = m.end()
        section_end = h2_matches[idx + 1].start() if idx + 1 < len(h2_matches) else len(raw_html)
        section_html = raw_html[section_start:section_end]

        if h2_text == "快讯" or h2_text.startswith("快讯"):
            kuaixun_start = section_start
            kuaixun_end = h2_matches[idx + 1].start() if idx + 1 < len(h2_matches) else len(raw_html)
            kuaixun_html = raw_html[kuaixun_start:kuaixun_end]

            paragraphs = _re.split(r'(?=<p[^>]*>\s*<strong)', kuaixun_html)
            for p_html in paragraphs:
                sm = strong_pat.search(p_html)
                if not sm:
                    continue
                kx_title = _strip_html(sm.group(1)).strip()
                if not kx_title:
                    continue
                kx_content = _strip_html(p_html).strip()
                kx_url = base_article.get("url", "")
                for hm in href_pat.finditer(p_html):
                    href = hm.group(1).strip().rstrip(".,;)]}>\"'")
                    if href and not _is_digest_internal_href(href):
                        kx_url = normalize_article_url(href)
                        break
                a = {**base_article}
                a["title"] = kx_title
                a["content"] = kx_content[:2000]
                a["summary"] = kx_content[:200]
                a["url"] = kx_url
                articles.append(a)
            continue

        title = _re.sub(r'^\d+\.\s*', '', h2_text).strip()
        if not title:
            continue

        content = _strip_html(section_html).strip()
        url = base_article.get("url", "")
        for hm in href_pat.finditer(section_html):
            href = hm.group(1).strip().rstrip(".,;)]}>\"'")
            if href and not _is_digest_internal_href(href):
                url = normalize_article_url(href)
                break

        a = {**base_article}
        a["title"] = title
        a["content"] = content[:2000]
        a["summary"] = content[:200]
        a["url"] = url
        articles.append(a)

    return articles if articles else [base_article]


def _first_source_href_from_digest_html(raw_html: str, fallback: str) -> str:
    """
    AI 资讯速览正文中「Sources:」后的第一个非本站外链；失败则退回全文首个外链，最后 fallback。
    """
    if not raw_html or not raw_html.strip():
        return fallback

    def _clean(u: str) -> str:
        u = (u or "").strip().rstrip(".,;)]}>\"'")
        return u

    low = raw_html.lower()
    key = "sources:"
    idx = low.find(key)
    window = raw_html[idx + len(key) :] if idx >= 0 else raw_html

    href_pat = _re.compile(r"""href\s*=\s*["'](https?://[^"'>\s]+)["']""", _re.I)
    for m in href_pat.finditer(window):
        h = _clean(m.group(1))
        if h and not _is_digest_internal_href(h):
            return h
    for m in href_pat.finditer(raw_html):
        h = _clean(m.group(1))
        if h and not _is_digest_internal_href(h):
            return h
    return fallback


def _http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 12) -> Optional[Dict]:
    request_headers = HEADERS.copy()
    if headers:
        request_headers.update(headers)
    req = Request(url, headers=request_headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _extract_hn_item_id(url: str) -> Optional[str]:
    m = _re.search(r"(?:item\?id=)(\d+)", url or "")
    return m.group(1) if m else None


def _extract_github_repo(url: str) -> Optional[str]:
    # 仅处理 GitHub 仓库链接
    m = _re.search(r"github\.com/([^/\s]+)/([^/\s?#]+)", url or "")
    if not m:
        return None
    owner, repo = m.group(1), m.group(2)
    if owner.lower() in {"topics", "trending", "search"}:
        return None
    return f"{owner}/{repo}".rstrip("/")


def _extract_bilibili_bvid(url: str) -> Optional[str]:
    m = _re.search(r"(BV[0-9A-Za-z]+)", url or "")
    return m.group(1) if m else None


def _extract_product_hunt_slug(url: str) -> Optional[str]:
    m = _re.search(r"producthunt\.com/posts/([a-zA-Z0-9-]+)", url or "")
    return m.group(1) if m else None


def _build_article(entry, feed_name: str, source_key: str) -> Optional[Dict]:
    title = getattr(entry, "title", "") or ""
    link = normalize_article_url(getattr(entry, "link", "") or "")
    content = _extract_content(entry)
    published = _parse_published(entry)
    if not title or not link:
        return None

    return {
        "title": title.strip(),
        "url": link.strip(),
        "content": content[:2000],
        "summary": content[:200] if content else title,
        "source": feed_name,
        "source_key": source_key,
        "is_ai_related": True,
        "published_at": published,
        "crawled_at": datetime.now().isoformat(),
        # 热度原始字段（后续 API 补齐）
        "heat_score": 0,
        "heat_comments": 0,
        "heat_views": 0,
        "heat_likes": 0,
        "heat_favorites": 0,
        "heat_source": "",
    }


def fetch_single_feed(feed_config: dict) -> List[Dict]:
    """
    拉取单个 RSS 源，返回文章列表
    格式与 NewsCollectorAgent.collect_news() 的输出一致
    """
    source_key = feed_config["source_key"]
    name = feed_config["name"]
    url = feed_config.get("rss_url", "") or feed_config.get("fallback_url", "")
    name = feed_config["name"]
    if not url:
        return []

    try:
        feed = feedparser.parse(url, request_headers=HEADERS)
        if feed.bozo and not feed.entries and feed_config.get("fallback_url"):
            fallback_url = feed_config["fallback_url"]
            feed = feedparser.parse(fallback_url, request_headers=HEADERS)
            logger.warning(f"  ⚠️ {name}: RSS 失败，回退到 {fallback_url}")

        if feed.bozo and not feed.entries:
            logger.warning(f"  ❌ {name}: 解析失败 - {feed.bozo_exception}")
            return []

        today_shanghai = datetime.now(TZ_SHANGHAI).date()

        max_articles = feed_config.get("max_articles", 0)
        entries = feed.entries if not max_articles else feed.entries[:max_articles]

        articles = []
        for feed_order, entry in enumerate(entries):
            article = _build_article(entry, name, source_key)
            if not article:
                continue
            if source_key == "ai_digest":
                try:
                    pub_dt = datetime.fromisoformat(article["published_at"])
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=TZ_SHANGHAI)
                    pub_date = pub_dt.astimezone(TZ_SHANGHAI).date()
                    if pub_date != today_shanghai:
                        continue
                except (ValueError, TypeError):
                    pass
                raw_html = _extract_raw_html_entry(entry)
                digest_page = article["url"]
                article["digest_page_url"] = digest_page
                sub_articles = _split_digest_html_into_articles(raw_html, article)
                for sub_order, sub_a in enumerate(sub_articles):
                    sub_a["ai_digest_feed_order"] = feed_order * 100 + sub_order
                    sub_a["source"] = name
                    sub_a["source_key"] = source_key
                articles.extend(sub_articles)
                continue
            articles.append(article)

        if source_key == "ai_digest":
            logger.info(f"  ✅ {name}: {len(articles)} 篇（仅当天 {today_shanghai}）")
        else:
            logger.info(f"  ✅ {name}: {len(articles)} 篇")
        return articles

    except Exception as e:
        logger.error(f"  ❌ {name}: 请求失败 - {e}")
        return []


def _api_fallback_hacker_news(limit: int = 20) -> List[Dict]:
    ids = _http_get_json("https://hacker-news.firebaseio.com/v0/topstories.json")
    if not isinstance(ids, list):
        return []
    articles: List[Dict] = []
    for item_id in ids[:limit]:
        item = _http_get_json(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json")
        if not item or item.get("type") != "story":
            continue
        title = str(item.get("title", "")).strip()
        url = normalize_article_url(
            str(item.get("url", "")).strip() or f"https://news.ycombinator.com/item?id={item_id}"
        )
        if not title or not url:
            continue
        articles.append(
            {
                "title": title,
                "url": url,
                "content": "",
                "summary": title,
                "source": "Hacker News",
                "source_key": "hacker_news",
                "is_ai_related": True,
                "published_at": datetime.fromtimestamp(item.get("time", datetime.now().timestamp())).isoformat(),
                "crawled_at": datetime.now().isoformat(),
                "heat_score": int(item.get("score", 0) or 0),
                "heat_comments": int(item.get("descendants", 0) or 0),
                "heat_views": 0,
                "heat_likes": 0,
                "heat_favorites": 0,
                "heat_source": "hn_api",
            }
        )
    return articles


def _api_fallback_github(limit: int = 20) -> List[Dict]:
    # API 兜底：直接用 GitHub Search API 当作热点候选
    data = _http_get_json(
        "https://api.github.com/search/repositories?q=topic:artificial-intelligence&sort=stars&order=desc&per_page=20"
    )
    items = (data or {}).get("items", [])
    articles: List[Dict] = []
    for repo in items[:limit]:
        title = f"{repo.get('full_name', '')}: {repo.get('description', '') or 'AI repo trending'}".strip()
        url = normalize_article_url(repo.get("html_url", ""))
        if not url:
            continue
        stars = int(repo.get("stargazers_count", 0) or 0)
        watchers = int(repo.get("watchers_count", 0) or 0)
        issues = int(repo.get("open_issues_count", 0) or 0)
        articles.append(
            {
                "title": title[:300],
                "url": url,
                "content": repo.get("description", "") or "",
                "summary": (repo.get("description", "") or "GitHub AI trending repository")[:200],
                "source": "GitHub Trending AI",
                "source_key": "github",
                "is_ai_related": True,
                "published_at": repo.get("pushed_at") or datetime.now().isoformat(),
                "crawled_at": datetime.now().isoformat(),
                "heat_score": int(stars * 1.0 + watchers * 0.5 + issues * 0.2),
                "heat_comments": issues,
                "heat_views": 0,
                "heat_likes": stars,
                "heat_favorites": watchers,
                "heat_source": "github_api",
            }
        )
    return articles


def _api_fallback_bilibili(limit: int = 20) -> List[Dict]:
    data = _http_get_json("https://api.bilibili.com/x/web-interface/ranking/v2?rid=0&type=all")
    items = (((data or {}).get("data") or {}).get("list") or [])
    articles: List[Dict] = []
    for item in items[:limit]:
        bvid = item.get("bvid", "")
        url = normalize_article_url(f"https://www.bilibili.com/video/{bvid}" if bvid else "")
        title = str(item.get("title", "")).strip()
        if not title or not url:
            continue
        stat = item.get("stat", {}) or {}
        views = int(stat.get("view", 0) or 0)
        likes = int(stat.get("like", 0) or 0)
        replies = int(stat.get("reply", 0) or 0)
        favorites = int(stat.get("favorite", 0) or 0)
        shares = int(stat.get("share", 0) or 0)
        coins = int(stat.get("coin", 0) or 0)
        desc = (item.get("desc") or "")[:1200]
        articles.append(
            {
                "title": title,
                "url": url,
                "content": desc,
                "summary": desc[:200] if desc else title,
                "source": "Bilibili 科技热榜",
                "source_key": "bilibili",
                "is_ai_related": True,
                "published_at": datetime.now().isoformat(),
                "crawled_at": datetime.now().isoformat(),
                "heat_score": int(views * 0.002 + likes + shares * 1.2 + replies * 0.8 + favorites * 0.8 + coins),
                "heat_comments": replies,
                "heat_views": views,
                "heat_likes": likes,
                "heat_favorites": favorites,
                "heat_source": "bilibili_api",
            }
        )
    return articles


def _api_fallback_product_hunt(limit: int = 20) -> List[Dict]:
    if not PRODUCT_HUNT_TOKEN:
        return []
    query = {
        "query": """
query TrendingPosts {
  posts(first: 20) {
    edges {
      node {
        name
        tagline
        slug
        votesCount
        commentsCount
        createdAt
      }
    }
  }
}
"""
    }
    req = Request(
        "https://api.producthunt.com/v2/api/graphql",
        data=json.dumps(query).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {PRODUCT_HUNT_TOKEN}",
            **HEADERS,
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, UnicodeDecodeError):
        return []
    edges = ((((data or {}).get("data") or {}).get("posts") or {}).get("edges") or [])
    articles: List[Dict] = []
    for edge in edges[:limit]:
        node = edge.get("node") or {}
        slug = node.get("slug")
        if not slug:
            continue
        votes = int(node.get("votesCount", 0) or 0)
        comments = int(node.get("commentsCount", 0) or 0)
        title = f"{node.get('name', '')} - {node.get('tagline', '')}".strip(" -")
        articles.append(
            {
                "title": title or slug,
                "url": normalize_article_url(f"https://www.producthunt.com/posts/{slug}"),
                "content": node.get("tagline", "") or "",
                "summary": (node.get("tagline", "") or title or slug)[:200],
                "source": "Product Hunt",
                "source_key": "product_hunt",
                "is_ai_related": True,
                "published_at": node.get("createdAt") or datetime.now().isoformat(),
                "crawled_at": datetime.now().isoformat(),
                "heat_score": int(votes + comments * 1.5),
                "heat_comments": comments,
                "heat_views": 0,
                "heat_likes": votes,
                "heat_favorites": 0,
                "heat_source": "producthunt_api",
            }
        )
    return articles


def api_fallback_by_source(source_key: str) -> List[Dict]:
    if source_key == "hacker_news":
        return _api_fallback_hacker_news()
    if source_key == "github":
        return _api_fallback_github()
    if source_key == "bilibili":
        return _api_fallback_bilibili()
    if source_key == "product_hunt":
        return _api_fallback_product_hunt()
    return []


def _enrich_hacker_news(article: Dict):
    item_id = _extract_hn_item_id(article.get("url", "")) or _extract_hn_item_id(article.get("summary", ""))
    if not item_id:
        comments_hint = article.get("content", "")
        item_id = _extract_hn_item_id(comments_hint)
    if not item_id:
        return

    data = _http_get_json(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json")
    if not data:
        return
    article["heat_score"] = int(data.get("score", 0) or 0)
    article["heat_comments"] = int(data.get("descendants", 0) or 0)
    article["heat_source"] = "hn_api"


def _enrich_github(article: Dict):
    repo = _extract_github_repo(article.get("url", ""))
    if not repo:
        return
    data = _http_get_json(f"https://api.github.com/repos/{repo}")
    if not data:
        return
    article["heat_likes"] = int(data.get("stargazers_count", 0) or 0)
    article["heat_favorites"] = int(data.get("watchers_count", 0) or 0)
    article["heat_comments"] = int(data.get("open_issues_count", 0) or 0)
    article["heat_score"] = int(
        article["heat_likes"] * 1.0 + article["heat_favorites"] * 0.5 + article["heat_comments"] * 0.2
    )
    article["heat_source"] = "github_api"


def _enrich_bilibili(article: Dict):
    bvid = _extract_bilibili_bvid(article.get("url", ""))
    if not bvid:
        return
    api = f"https://api.bilibili.com/x/web-interface/view?bvid={quote_plus(bvid)}"
    data = _http_get_json(api)
    if not data or data.get("code") != 0 or "data" not in data:
        return

    stat = data["data"].get("stat", {})
    views = int(stat.get("view", 0) or 0)
    likes = int(stat.get("like", 0) or 0)
    coins = int(stat.get("coin", 0) or 0)
    shares = int(stat.get("share", 0) or 0)
    replies = int(stat.get("reply", 0) or 0)
    favorites = int(stat.get("favorite", 0) or 0)

    article["heat_views"] = views
    article["heat_likes"] = likes
    article["heat_comments"] = replies
    article["heat_favorites"] = favorites
    article["heat_score"] = int(views * 0.002 + likes * 1.0 + coins * 1.0 + shares * 1.2 + replies * 0.8 + favorites * 0.8)
    article["heat_source"] = "bilibili_api"


def _enrich_product_hunt(article: Dict):
    if not PRODUCT_HUNT_TOKEN:
        return
    slug = _extract_product_hunt_slug(article.get("url", ""))
    if not slug:
        return

    query = {
        "query": """
query PostHeat($slug: String!) {
  post(slug: $slug) {
    votesCount
    commentsCount
  }
}
""",
        "variables": {"slug": slug},
    }
    req = Request(
        "https://api.producthunt.com/v2/api/graphql",
        data=json.dumps(query).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {PRODUCT_HUNT_TOKEN}",
            **HEADERS,
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, UnicodeDecodeError):
        return

    post = ((data or {}).get("data") or {}).get("post") or {}
    votes = int(post.get("votesCount", 0) or 0)
    comments = int(post.get("commentsCount", 0) or 0)
    if votes == 0 and comments == 0:
        return
    article["heat_likes"] = votes
    article["heat_comments"] = comments
    article["heat_score"] = int(votes * 1.0 + comments * 1.5)
    article["heat_source"] = "producthunt_api"


async def enrich_heat_metrics(articles: List[Dict]) -> List[Dict]:
    loop = asyncio.get_event_loop()

    def _enrich_one(article: Dict):
        source_key = article.get("source_key", "")
        if source_key == "hacker_news":
            _enrich_hacker_news(article)
        elif source_key == "github":
            _enrich_github(article)
        elif source_key == "bilibili":
            _enrich_bilibili(article)
        elif source_key == "product_hunt":
            _enrich_product_hunt(article)

    tasks = [loop.run_in_executor(None, _enrich_one, a) for a in articles]
    if tasks:
        await asyncio.gather(*tasks)
    return articles


async def collect_from_rss(feeds: List[Dict] = None) -> List[Dict]:
    """
    从所有 RSS 源拉取文章（失败时走 API 兜底）

    Returns:
        List[Dict]: 去重后的文章列表
    """
    active_feeds = feeds or TARGET_FEEDS

    print(f"\n{'='*50}")
    print(f"📡 RSS 采集开始: {len(active_feeds)} 个数据源")
    print(f"{'='*50}")

    all_articles = []

    print(f"\n📦 直连RSS + API兜底 ({len(active_feeds)} 个):")
    by_source: Dict[str, List[Dict]] = {f["source_key"]: [] for f in active_feeds}
    loop = asyncio.get_event_loop()
    rss_fallback_tasks = []
    for feed_config in active_feeds:
        task = loop.run_in_executor(None, fetch_single_feed, feed_config)
        rss_fallback_tasks.append((feed_config, task))

    for feed_config, task in rss_fallback_tasks:
        name = feed_config["name"]
        source_key = feed_config["source_key"]
        try:
            articles = await task
            if articles:
                by_source[source_key].extend(articles)
                print(f"  ✅ {name}: RSS 兜底成功 {len(articles)} 篇")
            else:
                api_articles = await loop.run_in_executor(None, api_fallback_by_source, source_key)
                by_source[source_key].extend(api_articles)
                if api_articles:
                    print(f"  ✅ {name}: API 兜底成功 {len(api_articles)} 篇")
                else:
                    print(f"  ❌ {name}: RSS/API 都失败")
        except Exception as e:
            print(f"  ❌ {name}: 兜底异常 - {e}")

    for source_items in by_source.values():
        all_articles.extend(source_items)

    # URL 去重
    seen_urls = set()
    unique_articles = []
    for a in all_articles:
        a["url"] = normalize_article_url(a.get("url", ""))
        if not a["url"]:
            continue
        if a["url"] not in seen_urls:
            seen_urls.add(a["url"])
            unique_articles.append(a)

    # 补充热度 API
    await enrich_heat_metrics(unique_articles)

    # 跨源热度可比：对 GitHub / HN / B站 / PH 等在组内做对数缩放或分位归一化
    apply_source_heat_calibration(unique_articles)

    digest_boosted = 0
    for a in unique_articles:
        if a.get("source_key") == "ai_digest":
            a["heat_score"] = AI_DIGEST_PRIORITY_HEAT
            a["heat_source"] = "ai_digest_priority"
            digest_boosted += 1
    if digest_boosted:
        print(f"  ⭐ AI资讯速览优先级: {digest_boosted} 篇 heat={AI_DIGEST_PRIORITY_HEAT}")

    success_count = len([1 for f in active_feeds if by_source.get(f["source_key"])])
    api_ok = len([a for a in unique_articles if a.get("heat_source")])
    print(f"\n📊 RSS 采集结果:")
    print(f"  数据源成功: {success_count}/{len(active_feeds)}")
    print(f"  总文章: {len(all_articles)} → 去重后: {len(unique_articles)}")
    print(f"  热度 API 已补齐: {api_ok} 篇")
    if HEAT_CALIBRATION not in ("", "none", "off", "false", "0", "no"):
        print(
            f"  热度校准: {HEAT_CALIBRATION} → 0..{int(HEAT_CAL_MAX)} "
            f"(源: {','.join(sorted(HEAT_CALIBRATION_SOURCES))})"
        )
    if any(f.get("source_key") == "product_hunt" for f in active_feeds) and not PRODUCT_HUNT_TOKEN:
        print("  ℹ️ Product Hunt 热度 API 未启用（缺少 PRODUCT_HUNT_TOKEN）")
    print(f"{'='*50}\n")

    return unique_articles
