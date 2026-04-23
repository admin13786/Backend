from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import httpx


def _base() -> str:
    return str(os.getenv("CRAWL_API_BASE", "http://localhost:8000")).rstrip("/")


async def fetch_rank_weibo(board: str, timeout_s: float = 12.0) -> List[Dict[str, Any]]:
    """
    Crawl API:
      GET /api/ranks/{board}/weibo
      board: main | sub
    """
    url = f"{_base()}/api/ranks/{board}/weibo"
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json() or {}
        lst = data.get("list") or []
        return lst if isinstance(lst, list) else []


async def fetch_news_by_id(news_id: int, timeout_s: float = 12.0) -> Optional[Dict[str, Any]]:
    """
    Crawl API:
      GET /news/{news_id}
    """
    url = f"{_base()}/news/{int(news_id)}"
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json() or {}
        if not data.get("success"):
            return None
        payload = data.get("data")
        return payload if isinstance(payload, dict) else None

