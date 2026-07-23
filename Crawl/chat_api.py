from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from chat_service import news_chat_service


chat_router = APIRouter(prefix="/news/chat", tags=["News Chat"])


class ChatMessageRequest(BaseModel):
    message: str = Field(..., description="用户输入")
    session_id: str = Field(default="", description="会话ID")
    limit: int = Field(default=6, ge=1, le=20)
    days: Optional[int] = Field(default=None, ge=1, le=30)
    source_keys: List[str] = Field(default_factory=list)
    history: List[Dict[str, Any]] = Field(default_factory=list)


class ChatSearchRequest(BaseModel):
    query: str = Field(..., description="检索词")
    limit: int = Field(default=8, ge=1, le=20)
    days: Optional[int] = Field(default=None, ge=1, le=30)
    source_keys: List[str] = Field(default_factory=list)


@chat_router.get("/sessions")
async def list_chat_sessions(limit: int = 20):
    result = await news_chat_service.list_sessions(limit=max(1, min(50, int(limit or 20))))
    return {"success": True, "data": result}


@chat_router.post("/message")
async def chat_message(req: ChatMessageRequest):
    result = await news_chat_service.handle_message(
        message=req.message,
        session_id=req.session_id,
        history=req.history,
        limit=req.limit,
        source_keys=req.source_keys,
        days=req.days,
    )
    return {"success": True, "data": result}


@chat_router.get("/session/{session_id}")
async def get_chat_session(session_id: str):
    result = await news_chat_service.get_session(session_id)
    if not result:
        return {"success": False, "message": "session not found"}
    return {"success": True, "data": result}


@chat_router.post("/search")
async def preview_chat_search(req: ChatSearchRequest):
    result = await news_chat_service.preview_search(
        query=req.query,
        limit=req.limit,
        source_keys=req.source_keys,
        days=req.days,
    )
    return {"success": True, "data": result}
