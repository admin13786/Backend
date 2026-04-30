"""
排行榜 + 登录接口
对接前端接口文档的 RESTful API
"""

import json
from urllib.parse import urljoin

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from db import (
    create_session,
    create_user,
    delete_session,
    delete_workshop_conversation,
    get_all_batches,
    get_top_articles,
    get_session_user,
    get_user_by_username,
    hash_password,
    list_workshop_conversations,
    upsert_workshop_conversation,
    verify_password,
)

rank_router = APIRouter(prefix="/api", tags=["排行榜"])


# ========== 登录接口 ==========

class LoginRequest(BaseModel):
    username: str
    password: str


class RegisterRequest(BaseModel):
    username: str
    password: str
    display_name: str = ""


class WorkshopConversationPayload(BaseModel):
    id: str
    title: str = "新对话"
    conversationMode: str = "workshop"
    orderIndex: int | None = None
    createdAt: str | None = None
    updatedAt: str | None = None
    messages: list = Field(default_factory=list)
    selectedSkills: list = Field(default_factory=list)
    preview: dict = Field(default_factory=dict)


def _issue_token(username: str) -> str:
    import secrets

    return secrets.token_hex(32)


async def _require_user(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="未登录或登录已失效")
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="未登录或登录已失效")
    user = await get_session_user(token)
    if not user:
        raise HTTPException(status_code=401, detail="未登录或登录已失效")
    return user, token


def _build_user_payload(user: dict | None) -> dict:
    payload = user or {}
    username = str(payload.get("username") or "").strip()
    role = str(payload.get("role") or "user").strip().lower() or "user"
    return {
        "username": username,
        "displayName": str(payload.get("display_name") or payload.get("displayName") or username).strip() or username,
        "role": role,
        "isAdmin": role == "admin",
    }


@rank_router.post("/auth/sessions")
async def login(req: LoginRequest):
    username = str(req.username or "").strip()
    user = await get_user_by_username(username)
    if user and verify_password(req.password, user["password_hash"]):
        token = _issue_token(username)
        await create_session(username, token)
        return {
            "success": True,
            "token": token,
            "user": _build_user_payload(user),
        }
    raise HTTPException(status_code=401, detail="用户名或密码错误")


@rank_router.post("/auth/register")
async def register(req: RegisterRequest):
    username = str(req.username or "").strip()
    password = str(req.password or "")
    display_name = str(req.display_name or username).strip()

    if len(username) < 1:
        raise HTTPException(status_code=400, detail="用户名不能为空")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="密码至少需要 6 个字符")

    exists = await get_user_by_username(username)
    if exists:
        raise HTTPException(status_code=409, detail="用户名已存在")

    user = await create_user(username, password, display_name)
    token = _issue_token(username)
    await create_session(username, token)
    return {
        "success": True,
        "token": token,
        "user": _build_user_payload(user),
    }


@rank_router.get("/auth/me")
async def auth_me(authorization: str | None = Header(default=None)):
    user, _ = await _require_user(authorization)
    return {
        "success": True,
        "user": _build_user_payload(user),
    }


@rank_router.delete("/auth/sessions/current")
async def logout(authorization: str | None = Header(default=None)):
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
        await delete_session(token)
    return {"success": True}


@rank_router.get("/workshop-history/conversations")
async def get_workshop_conversations(authorization: str | None = Header(default=None)):
    user, _ = await _require_user(authorization)
    conversations = await list_workshop_conversations(user["username"])
    return {"success": True, "list": conversations}


@rank_router.put("/workshop-history/conversations/{conversation_id}")
async def save_workshop_conversation(
    conversation_id: str,
    payload: WorkshopConversationPayload,
    authorization: str | None = Header(default=None),
):
    user, _ = await _require_user(authorization)
    if conversation_id != payload.id:
        raise HTTPException(status_code=400, detail="会话 id 不一致")
    conversation = await upsert_workshop_conversation(user["username"], payload.model_dump())
    return {"success": True, "data": conversation}


@rank_router.delete("/workshop-history/conversations/{conversation_id}")
async def remove_workshop_conversation(
    conversation_id: str,
    authorization: str | None = Header(default=None),
):
    user, _ = await _require_user(authorization)
    await delete_workshop_conversation(user["username"], conversation_id)
    return {
        "success": True,
        "deleted": True,
        "conversationId": conversation_id,
    }


# ========== 排行榜接口 ==========

async def _articles_for_board(board: str):
    """main → 企业榜(biz_*)，sub → 个人榜(personal_*)。"""
    if board == "main":
        return await get_top_articles(limit=60, board="business")
    elif board == "sub":
        return await get_top_articles(limit=60, board="personal")
    return await get_top_articles(limit=60, board="business")


def _absolute_media_url(url: str, request: Request) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return urljoin(str(request.base_url), raw.lstrip("/"))


def _extract_article_brief(article: dict) -> dict:
    stored_brief = str(article.get("brief_json", "") or "").strip()
    if not stored_brief:
        return {}
    try:
        brief = json.loads(stored_brief)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return brief if isinstance(brief, dict) else {}


def _extract_article_overview(article: dict) -> str:
    brief = _extract_article_brief(article)
    lead = str(brief.get("lead", "") or "").strip()
    if lead:
        return lead

    paragraphs = brief.get("paragraphs") or []
    if isinstance(paragraphs, list):
        for paragraph in paragraphs:
            text = str(paragraph or "").strip()
            if text:
                return text

    summary = str(article.get("summary", "") or "").strip()
    if summary:
        return summary

    content = str(article.get("content", "") or "").strip()
    if not content:
        return ""
    return content[:140].rstrip() + ("..." if len(content) > 140 else "")


def _build_highlight_item(article: dict, board: str, rank_index: int, request: Request) -> dict:
    return {
        "board": board,
        "rank": rank_index,
        "newsId": article.get("id"),
        "title": article.get("title", ""),
        "source": article.get("source", ""),
        "sourceKey": article.get("source_key", ""),
        "url": article.get("url", ""),
        "coverUrl": _absolute_media_url(article.get("cover_url", ""), request),
        "summary": str(article.get("summary", "") or "").strip(),
        "overview": _extract_article_overview(article),
        "publishedAt": article.get("published_at"),
        "score": article.get("total_score", 0),
        "brief": _extract_article_brief(article),
    }


@rank_router.get("/ranks/{board}/weibo")
async def get_weibo_rank(board: str):
    """
    主榜 (board=main): 企业新榜（biz_* 批次）
    副榜 (board=sub): 个人新榜（personal_* 批次）
    """
    articles = await _articles_for_board(board)
    if not articles:
        return {"list": []}

    result = []
    for i, a in enumerate(articles, 1):
        source = a.get("source", "")
        source_key = a.get("source_key", "")
        tag = "AI"
        if source_key == "ai_digest":
            tag = "速览"
        result.append({
            "id": i,
            "title": a.get("title", ""),
            "viewsNum": str(round(a.get("total_score", 0), 1)),
            "tag": tag,
            "newsId": a.get("id"),
            "url": a.get("url"),
            "source": source,
            "coverUrl": a.get("cover_url", ""),
            "summary": _extract_article_overview(a),
            "overview": _extract_article_overview(a),
            "brief": _extract_article_brief(a),
        })
    return {"list": result}


@rank_router.get("/ranks/{board}/video")
async def get_video_rank(board: str):
    """视频榜由前端 Mock；此处保留空列表供可选直连。"""
    return {"list": []}


@rank_router.get("/ranks/highlights")
async def get_rank_highlights(
    request: Request,
    limit: int = Query(default=2, ge=1, le=10),
):
    """返回两个排行榜各自前 N 条新闻的聚合信息，适合安卓端直接读取。"""
    main_articles = await _articles_for_board("main")
    sub_articles = await _articles_for_board("sub")

    main_list = [
        _build_highlight_item(article, "main", index, request)
        for index, article in enumerate(main_articles[:limit], 1)
    ]
    sub_list = [
        _build_highlight_item(article, "sub", index, request)
        for index, article in enumerate(sub_articles[:limit], 1)
    ]

    return {
        "success": True,
        "limit": limit,
        "boards": {
            "main": {
                "name": "business",
                "list": main_list,
            },
            "sub": {
                "name": "personal",
                "list": sub_list,
            },
        },
    }


@rank_router.get("/ranks/batches")
async def list_batches():
    """获取所有爬取批次（扩展接口）"""
    batches = await get_all_batches()
    return {"batches": batches}
