from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import quote

from fastapi import APIRouter, Query
from fastapi.responses import Response

from cover_renderer_xhs import render_cover_png
from crawl_client import fetch_news_by_id, fetch_rank_weibo
from db import (
    get_article,
    get_stats,
    init_db,
    list_pending_articles,
    list_ready_articles,
    mark_llm_error,
    mark_llm_processing,
    reset_article_to_pending,
    save_processed_article,
    save_raw_article,
)
from edu_logic import edu_score, extract_concepts, generate_hook_title, pick_template_id
from llm_client import popularize_article

edu_router = APIRouter(prefix="", tags=["EduRepo"])

init_db()

_DATA_DIR = Path(os.getenv("EDU_REPO_DATA_DIR", str(Path(__file__).resolve().parent / "data")))
_COVER_DIR = _DATA_DIR / "covers"
_COVER_DIR.mkdir(parents=True, exist_ok=True)

_PROCESS_TASK: asyncio.Task | None = None


def _start_process_task(coro: "asyncio.Future[Any]") -> bool:
    global _PROCESS_TASK
    if _PROCESS_TASK is not None and not _PROCESS_TASK.done():
        return False
    task = asyncio.create_task(coro)
    _PROCESS_TASK = task

    def _done(_t: asyncio.Task) -> None:
        global _PROCESS_TASK
        _PROCESS_TASK = None

    task.add_done_callback(_done)
    return True


def _cover_cache_key(template_id: str, title: str, highlights: List[str], size: str) -> str:
    raw = "|".join([template_id, size, title, ",".join(highlights or [])])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _text_blob(obj: Dict[str, Any]) -> str:
    return "\n".join(
        [
            str(obj.get("ps_title") or obj.get("title") or ""),
            str(obj.get("ps_summary") or obj.get("summary") or ""),
            str(obj.get("ps_markdown") or ""),
            str(obj.get("title") or ""),
            str(obj.get("summary") or ""),
            str(obj.get("content") or ""),
            str(obj.get("source") or ""),
            str(obj.get("url") or ""),
        ]
    )


def _build_cover_url(template_id: str, title: str, highlights: List[str], size: str = "3x4") -> str:
    hl = [str(x or "").strip() for x in (highlights or []) if str(x or "").strip()][:3]
    t = (title or "").strip()
    return (
        "/api/edu/cover.png"
        f"?templateId={quote(template_id, safe='')}"
        f"&size={quote(size, safe='')}"
        f"&title={quote(t, safe='')}"
        f"&highlights={quote(','.join(hl), safe=',')}"
    )


def _pick_highlights(article: Dict[str, Any], concepts: List[str]) -> List[str]:
    hl = article.get("highlights_json") or article.get("highlights") or []
    if isinstance(hl, str):
        hl = [s.strip() for s in hl.split(",") if s.strip()]
    if not isinstance(hl, list):
        hl = []
    hl2 = [str(x or "").strip() for x in hl if str(x or "").strip()]
    if hl2:
        return hl2[:3]

    kw = article.get("keywords_json") or article.get("keywords") or []
    if isinstance(kw, str):
        kw = [s.strip() for s in kw.split(",") if s.strip()]
    if isinstance(kw, list):
        kw2 = [str(x or "").strip() for x in kw if str(x or "").strip()]
        if kw2:
            return kw2[:3]

    return [c for c in concepts[:3] if c]


async def _sync_from_crawl(
    limit: int, board: str, q: str, min_score: float, fetch_concurrency: int = 10
) -> Dict[str, Any]:
    ranks: list[dict] = []
    main_ids: set[int] = set()
    sub_ids: set[int] = set()
    if board in ("all", "main"):
        main = await fetch_rank_weibo("main")
        ranks.extend(main)
        for it in main:
            if it.get("newsId") is None:
                continue
            try:
                main_ids.add(int(it["newsId"]))
            except Exception:
                continue
    if board in ("all", "sub"):
        sub = await fetch_rank_weibo("sub")
        ranks.extend(sub)
        for it in sub:
            if it.get("newsId") is None:
                continue
            try:
                sub_ids.add(int(it["newsId"]))
            except Exception:
                continue

    keyword = str(q or "").strip().lower()
    candidates: list[Tuple[int, bool, bool]] = []
    seen = set()
    for it in ranks:
        news_id = it.get("newsId")
        if news_id is None:
            continue
        nid = int(news_id)
        k = str(nid)
        if k in seen:
            continue
        seen.add(k)
        title = str(it.get("title") or "")
        if keyword and keyword not in title.lower():
            continue
        if edu_score(title) < float(min_score) * 0.65:
            continue
        candidates.append((nid, nid in main_ids, nid in sub_ids))
        if len(candidates) >= int(limit):
            break

    sem = asyncio.Semaphore(max(1, int(fetch_concurrency)))

    async def _load_one(nid: int) -> dict:
        async with sem:
            return await fetch_news_by_id(int(nid)) or {}

    details = await asyncio.gather(*[_load_one(nid) for nid, _, _ in candidates], return_exceptions=True)
    saved = 0
    changed = 0
    pending = 0
    done = 0
    failed: list[int] = []
    for (nid, to_main, to_sub), detail in zip(candidates, details):
        if isinstance(detail, Exception) or not isinstance(detail, dict) or not detail:
            failed.append(int(nid))
            continue
        try:
            ch, st = save_raw_article(detail, in_main=bool(to_main), in_sub=bool(to_sub))
            saved += 1
            if ch:
                changed += 1
            if st == "pending":
                pending += 1
            if st == "done":
                done += 1
        except Exception:
            failed.append(int(nid))

    return {
        "candidates": len(candidates),
        "saved": saved,
        "changed": changed,
        "pending": pending,
        "done": done,
        "failed": failed[:20],
    }


async def _process_rows(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    ok = 0
    failed: list[int] = []

    # keep low concurrency to protect LLM provider
    sem = asyncio.Semaphore(2)

    async def _proc_one(row: Dict[str, Any]) -> None:
        nonlocal ok, failed
        nid = int(row.get("news_id") or row.get("newsId") or row.get("id") or 0)
        if not nid:
            return
        raw = row.get("raw_json") or {}
        if not isinstance(raw, dict) or not raw.get("title"):
            mark_llm_error(nid, "missing raw_json/title; run /api/edu/sync first")
            failed.append(nid)
            return

        async with sem:
            try:
                attempts = mark_llm_processing(nid)
                if attempts > 3:
                    mark_llm_error(nid, "attempts exceeded")
                    failed.append(nid)
                    return

                ps = await popularize_article(raw)
                if not ps.get("ps_summary") or not ps.get("ps_markdown"):
                    raise ValueError("LLM returned empty ps fields")
                save_processed_article(nid, ps)
                ok += 1
            except Exception as e:
                mark_llm_error(nid, str(e))
                failed.append(nid)

    await asyncio.gather(*[_proc_one(row) for row in items])
    return {"picked": len(items), "ok": ok, "failed": failed[:20]}


async def _process_pending(limit: int, board: str) -> Dict[str, Any]:
    items = list_pending_articles(limit=int(limit), board=board)
    return await _process_rows(items)


@edu_router.get("/api/edu/feed")
async def feed(
    limit: int = Query(default=40, ge=1, le=120),
    board: str = Query(default="all", pattern="^(all|main|sub)$"),
    q: str = Query(default="", max_length=80),
    minScore: float = Query(default=1.4, ge=0, le=20),
):
    """
    Feed is DB-first: only returns records already processed by LLM (status=done).
    Use POST /api/edu/sync + POST /api/edu/process (or /api/edu/backfill) to update the DB.
    """
    rows = list_ready_articles(limit=max(80, int(limit) * 3), board=board, q=q)
    refined: list[dict] = []
    for row in rows:
        blob = _text_blob(row)
        s = edu_score(blob)
        if s < float(minScore):
            continue
        concepts = extract_concepts(blob, top_k=4)
        stable_key = str(row.get("url") or row.get("news_id") or "")
        display_title = str(row.get("ps_title") or row.get("title") or "")
        hook, gen_hl = generate_hook_title(display_title, concepts, stable_key)
        title = str(row.get("ps_title") or hook or display_title).strip() or hook
        hl = _pick_highlights(row, concepts) or gen_hl
        template_id = pick_template_id(stable_key)
        cover_url = _build_cover_url(template_id, title, hl, size="3x4")

        refined.append(
            {
                "newsId": int(row.get("news_id") or 0),
                "url": str(row.get("url") or ""),
                "source": str(row.get("source") or ""),
                "publishedAt": str(row.get("published_at") or ""),
                "originalTitle": str(row.get("title") or ""),
                "hookTitle": title,
                "highlights": hl,
                "templateId": template_id,
                "summary": str(row.get("ps_summary") or row.get("summary") or ""),
                "keywords": row.get("keywords_json") if isinstance(row.get("keywords_json"), list) else [],
                "glossary": row.get("glossary_json") if isinstance(row.get("glossary_json"), list) else [],
                "eduScore": round(float(s), 3),
                "coverUrl": cover_url,
                "externalCoverUrl": str(row.get("cover_url") or ""),
                "hasPs": True,
            }
        )
        if len(refined) >= int(limit):
            break

    refined.sort(key=lambda x: (x.get("eduScore") or 0), reverse=True)
    return {"success": True, "list": refined[:limit]}


@edu_router.get("/api/edu/stats")
async def stats():
    running = bool(_PROCESS_TASK is not None and not _PROCESS_TASK.done())
    return {"success": True, "stats": {**get_stats(), "bgProcessing": running}}


@edu_router.post("/api/edu/sync")
async def sync(
    limit: int = Query(default=80, ge=1, le=200),
    board: str = Query(default="all", pattern="^(all|main|sub)$"),
    q: str = Query(default="", max_length=80),
    minScore: float = Query(default=1.2, ge=0, le=20),
):
    """
    Sync raw articles from Crawl into EduRepo DB (no LLM).
    """
    result = await _sync_from_crawl(limit=int(limit), board=board, q=q, min_score=float(minScore))
    return {"success": True, "result": result}


@edu_router.post("/api/edu/process")
async def process(
    limit: int = Query(default=20, ge=1, le=60),
    board: str = Query(default="all", pattern="^(all|main|sub)$"),
    asyncMode: bool = Query(default=False),
):
    """
    Process pending/error records using LLM and write ps_* fields into DB.
    """
    if asyncMode:
        items = list_pending_articles(limit=int(limit), board=board)
        if not items:
            return {"success": True, "started": 0, "message": "no pending items"}
        started = _start_process_task(_process_rows(items))
        return {"success": True, "started": len(items), "alreadyRunning": (not started)}

    result = await _process_pending(limit=int(limit), board=board)
    return {"success": True, "result": result}


@edu_router.post("/api/edu/backfill")
async def backfill(
    limit: int = Query(default=20, ge=1, le=60),
    board: str = Query(default="all", pattern="^(all|main|sub)$"),
    q: str = Query(default="", max_length=80),
    minScore: float = Query(default=1.2, ge=0, le=20),
    asyncMode: bool = Query(default=False),
):
    """
    Convenience endpoint:
      1) sync raw from Crawl
      2) process pending via LLM
    """
    sync_res = await _sync_from_crawl(limit=max(int(limit) * 3, 60), board=board, q=q, min_score=float(minScore))
    if asyncMode:
        items = list_pending_articles(limit=int(limit), board=board)
        if not items:
            return {"success": True, "sync": sync_res, "started": 0, "message": "no pending items"}
        started = _start_process_task(_process_rows(items))
        return {"success": True, "sync": sync_res, "started": len(items), "alreadyRunning": (not started)}

    proc_res = await _process_pending(limit=int(limit), board=board)
    return {"success": True, "sync": sync_res, "process": proc_res}


@edu_router.get("/api/edu/items/{news_id}")
async def item(news_id: int):
    row = get_article(int(news_id))
    if not row:
        return {"success": False, "message": f"not found: {news_id}"}

    status = str(row.get("status") or "")
    blob = _text_blob(row)
    concepts = extract_concepts(blob, top_k=6)
    stable_key = str(row.get("url") or news_id)

    display_title = str(row.get("ps_title") or row.get("title") or "")
    hook, gen_hl = generate_hook_title(display_title, concepts, stable_key)
    title = str(row.get("ps_title") or hook or display_title).strip() or hook
    hl = _pick_highlights(row, concepts) or gen_hl
    template_id = pick_template_id(stable_key)
    cover_url = _build_cover_url(template_id, title, hl, size="3x4")

    ps_markdown = str(row.get("ps_markdown") or "").strip()
    if len(ps_markdown) > 12000:
        ps_markdown = ps_markdown[:12000].rstrip() + "..."

    return {
        "success": True,
        "data": {
            "newsId": int(row.get("news_id") or news_id),
            "status": status,
            "url": str(row.get("url") or ""),
            "source": str(row.get("source") or ""),
            "publishedAt": str(row.get("published_at") or ""),
            "originalTitle": str(row.get("title") or ""),
            "hookTitle": title,
            "highlights": hl,
            "templateId": template_id,
            "summary": str(row.get("ps_summary") or row.get("summary") or ""),
            "psMarkdown": ps_markdown,
            "keywords": row.get("keywords_json") if isinstance(row.get("keywords_json"), list) else [],
            "glossary": row.get("glossary_json") if isinstance(row.get("glossary_json"), list) else [],
            "concepts": concepts,
            "coverUrl": cover_url,
            "externalCoverUrl": str(row.get("cover_url") or ""),
            "llmError": str(row.get("llm_error") or ""),
        },
    }


@edu_router.post("/api/edu/items/{news_id}/reprocess")
async def reprocess_item(news_id: int, asyncMode: bool = Query(default=False)):
    """
    Re-run LLM rewriting for a single item.
    Useful after tuning the rewrite prompt/normalization logic.
    """
    row = get_article(int(news_id))
    if not row:
        return {"success": False, "message": f"not found: {news_id}"}
    raw = row.get("raw_json") or {}
    if not isinstance(raw, dict) or not raw.get("title"):
        return {"success": False, "message": "missing raw_json/title; run /api/edu/sync first"}

    reset_article_to_pending(int(news_id))

    async def _run_once() -> dict:
        try:
            mark_llm_processing(int(news_id))
            ps = await popularize_article(raw)
            if not ps.get("ps_summary") or not ps.get("ps_markdown"):
                raise ValueError("LLM returned empty ps fields")
            save_processed_article(int(news_id), ps)
            return {"success": True, "newsId": int(news_id), "llm_model": ps.get("llm_model") or ""}
        except Exception as e:
            mark_llm_error(int(news_id), str(e))
            return {"success": False, "newsId": int(news_id), "message": str(e)}

    if asyncMode:
        started = _start_process_task(_run_once())
        return {"success": True, "started": 1 if started else 0, "alreadyRunning": (not started), "newsId": int(news_id)}

    return await _run_once()


@edu_router.get("/api/edu/cover.png")
async def cover_png(
    templateId: str = Query(default="t1", max_length=8),
    title: str = Query(default="", max_length=120),
    highlights: str = Query(default="", max_length=120),
    size: str = Query(default="3x4", pattern="^(3x4|1x1|9x16)$"),
):
    hl = [s.strip() for s in (highlights or "").split(",") if s.strip()][:3]
    t = (title or "").strip()
    if not t:
        t = "3分钟搞懂：大模型新概念"

    cache_key = _cover_cache_key(templateId, t, hl, size)
    path = _COVER_DIR / f"{cache_key}.png"
    if path.exists():
        return Response(content=path.read_bytes(), media_type="image/png")

    png = render_cover_png(templateId, t, hl, size=size)
    try:
        path.write_bytes(png)
    except Exception:
        pass
    return Response(content=png, media_type="image/png")
