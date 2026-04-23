"""
一次性回填历史新闻封面：
- 查询 cover_url 为空的文章
- 抓取封面并上传 OSS
- 将 OSS URL 回写到 news_articles.cover_url

示例：
  python3 backfill_cover_urls.py
  python3 backfill_cover_urls.py --limit 100 --concurrency 4
  python3 backfill_cover_urls.py --domain openai.com --force
"""

import argparse
import asyncio
import os
import sys
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import close_pool, init_db  # noqa: E402
import db as db_module  # noqa: E402
from cover_service import enrich_articles_with_covers  # noqa: E402


async def _fetch_pending_articles(limit: int, domain: str = "", force: bool = False) -> List[Dict]:
    conn = db_module._conn
    if conn is None:
        return []
    domain = (domain or "").strip().lower()
    where_parts = []
    params: List[object] = []
    if not force:
        where_parts.append("TRIM(COALESCE(cover_url, '')) = ''")
    if domain:
        # SQLite 大小写不敏感匹配域名或子域
        where_parts.append("LOWER(url) LIKE ?")
        params.append(f"%{domain}%")
    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    cursor = await conn.cursor()
    await cursor.execute(
        f"""
        SELECT id, url, title, cover_url
        FROM news_articles
        WHERE {where_sql}
        ORDER BY id ASC
        LIMIT ?
        """,
        tuple([*params, limit]),
    )
    rows = await cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


async def _save_cover_url(article_id: int, cover_url: str) -> None:
    conn = db_module._conn
    if conn is None:
        return
    await conn.execute(
        """
        UPDATE news_articles
        SET cover_url = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (cover_url, article_id),
    )


async def backfill(limit: int, concurrency: int, domain: str = "", force: bool = False) -> int:
    pending = await _fetch_pending_articles(limit, domain=domain, force=force)
    if not pending:
        print("✅ 没有待回填的历史文章")
        return 0

    scope = f" domain={domain}" if domain else ""
    mode = "force" if force else "empty-only"
    print(f"🧾 待回填文章数: {len(pending)} ({mode}{scope})")

    if force:
        # 让 enrich 阶段不因为已有 cover_url 而跳过
        for article in pending:
            article["cover_url"] = ""

    await enrich_articles_with_covers(pending, concurrency=concurrency)

    updated = 0
    for article in pending:
        cover_url = str(article.get("cover_url", "") or "").strip()
        if not cover_url:
            continue
        await _save_cover_url(int(article["id"]), cover_url)
        updated += 1

    if db_module._conn is not None:
        await db_module._conn.commit()

    failed = len(pending) - updated
    print(f"✅ 回填完成: 成功 {updated} 篇, 失败 {failed} 篇")
    return updated


async def main() -> None:
    parser = argparse.ArgumentParser(description="回填历史新闻 cover_url")
    parser.add_argument("--limit", type=int, default=500, help="本次最多处理多少条，默认 500")
    parser.add_argument("--concurrency", type=int, default=6, help="并发抓取数，默认 6")
    parser.add_argument(
        "--domain",
        type=str,
        default="",
        help="仅处理 URL 包含该域名关键词的文章，例如 openai.com",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="强制重刷（即使已有 cover_url 也重生成并覆盖）",
    )
    args = parser.parse_args()

    await init_db()
    try:
        await backfill(
            limit=max(1, args.limit),
            concurrency=max(1, args.concurrency),
            domain=args.domain,
            force=args.force,
        )
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
