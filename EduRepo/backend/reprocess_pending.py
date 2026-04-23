from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from db import (
    list_pending_articles,
    mark_llm_error,
    mark_llm_processing,
    save_processed_article,
)
from llm_client import popularize_article


async def _process_batch(items: List[Dict[str, Any]], concurrency: int = 2) -> Dict[str, Any]:
    ok = 0
    failed: list[int] = []
    sem = asyncio.Semaphore(max(1, int(concurrency)))

    async def _one(row: Dict[str, Any]) -> None:
        nonlocal ok, failed
        nid = int(row.get("news_id") or row.get("newsId") or row.get("id") or 0)
        if not nid:
            return
        raw = row.get("raw_json") or {}
        if not isinstance(raw, dict) or not raw.get("title"):
            mark_llm_error(nid, "missing raw_json/title")
            failed.append(nid)
            return

        async with sem:
            try:
                mark_llm_processing(nid)
                ps = await popularize_article(raw)
                if not ps.get("ps_summary") or not ps.get("ps_markdown"):
                    raise ValueError("LLM returned empty ps fields")
                save_processed_article(nid, ps)
                ok += 1
            except Exception as e:
                mark_llm_error(nid, str(e))
                failed.append(nid)

    await asyncio.gather(*[_one(row) for row in items])
    return {"picked": len(items), "ok": ok, "failed": failed[:20]}


async def main() -> None:
    batch_size = 20
    concurrency = 2
    total_ok = 0
    rounds = 0

    while True:
        items = list_pending_articles(limit=batch_size, board="all")
        if not items:
            break
        rounds += 1
        res = await _process_batch(items, concurrency=concurrency)
        total_ok += int(res.get("ok") or 0)
        print(f"[round {rounds}] picked={res.get('picked')} ok={res.get('ok')} failed={res.get('failed')}")

    print(f"done. total_ok={total_ok}")


if __name__ == "__main__":
    asyncio.run(main())

