from typing import List

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from db import (
    get_latest_push_brief_issue,
    get_push_brief_issue_by_id,
    get_session_user,
    list_email_subscribers,
    list_push_devices,
    unsubscribe_email_subscriber_by_token,
    upsert_email_subscriber,
    upsert_push_device,
)
from email_service import (
    get_email_config_status,
    send_issue_to_email_subscribers,
)
from push_service import (
    build_daily_highlights,
    create_selected_issue,
    is_push_configured,
    send_daily_highlights_to_registered_devices,
    send_notification_to_client_ids,
    send_selected_issue_to_registered_devices,
)
from wechat_mp_service import (
    get_wechat_mp_config_status,
    publish_issue_to_wechat,
    sync_wechat_publish_status,
)


push_router = APIRouter(prefix="/api/push", tags=["push"])


class PushRegisterRequest(BaseModel):
    clientId: str
    userId: str = ""
    platform: str = "android"
    deviceName: str = ""
    pushEnabled: bool = True


class PushTestRequest(BaseModel):
    clientId: str = ""
    title: str = "测试通知"
    body: str = "这是一条来自后端的测试通知"
    route: str = "/pages/news-brief/issue"


class PushSelectedRequest(BaseModel):
    articleIds: List[int] = Field(default_factory=list)
    createdBy: str = ""


class EmailSubscriberRequest(BaseModel):
    email: str
    userId: str = ""
    displayName: str = ""
    source: str = "manual"
    verified: bool = False


async def _resolve_request_user(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        return ""
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        return ""
    user = await get_session_user(token)
    if not user:
        return ""
    return str(user.get("username") or user.get("id") or "").strip()


def _normalize_user_role(user: dict | None) -> str:
    payload = user or {}
    role = str(payload.get("role") or "").strip().lower()
    username = str(payload.get("username") or "").strip().lower()
    if role == "admin" or username == "admin":
        return "admin"
    return "user"


async def _resolve_request_session_user(authorization: str | None) -> dict | None:
    if not authorization or not authorization.lower().startswith("bearer "):
        return None
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        return None
    user = await get_session_user(token)
    if not user:
        return None
    return {
        **user,
        "role": _normalize_user_role(user),
        "token": token,
    }


async def _require_admin_user(authorization: str | None = Header(default=None)) -> dict:
    user = await _resolve_request_session_user(authorization)
    if not user:
        raise HTTPException(status_code=401, detail="admin authentication required")
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="admin access required")
    return user


@push_router.get("/config")
async def get_push_config_status():
    devices = await list_push_devices(platform="android", push_enabled_only=False)
    enabled_count = sum(1 for item in devices if item.get("push_enabled"))
    return {
        "success": True,
        "configured": is_push_configured(),
        "androidDeviceCount": len(devices),
        "androidEnabledCount": enabled_count,
    }


@push_router.get("/wechat/config")
async def get_wechat_config_status():
    return {"success": True, "data": get_wechat_mp_config_status()}


@push_router.get("/email/config")
async def get_email_channel_config_status():
    return {"success": True, "data": await get_email_config_status()}


@push_router.get("/email/subscribers")
async def get_email_subscriber_list(
    activeOnly: bool = Query(default=False),
    _admin_user: dict = Depends(_require_admin_user),
):
    subscribers = await list_email_subscribers(active_only=activeOnly)
    return {"success": True, "data": subscribers}


@push_router.post("/email/subscribers")
async def create_or_update_email_subscriber(payload: EmailSubscriberRequest):
    try:
        subscriber = await upsert_email_subscriber(
            payload.email,
            user_id=payload.userId,
            display_name=payload.displayName,
            source=payload.source,
            verified=payload.verified,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"success": True, "data": subscriber}


@push_router.get("/email/unsubscribe")
async def unsubscribe_email_subscriber(token: str = Query(default="")):
    subscriber = await unsubscribe_email_subscriber_by_token(token)
    if not subscriber:
        raise HTTPException(status_code=404, detail="subscriber not found")
    return {"success": True, "data": subscriber}


@push_router.post("/register")
async def register_push_device(
    payload: PushRegisterRequest,
    authorization: str | None = Header(default=None),
):
    try:
        resolved_user_id = await _resolve_request_user(authorization)
        device = await upsert_push_device(
            client_id=payload.clientId,
            user_id=resolved_user_id or payload.userId,
            platform=payload.platform,
            device_name=payload.deviceName,
            push_enabled=payload.pushEnabled,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"success": True, "data": device}


@push_router.get("/daily-preview")
async def get_daily_push_preview(limit: int = Query(default=2, ge=1, le=10)):
    digest = await build_daily_highlights(limit=limit)
    return {"success": True, "data": digest}


@push_router.post("/test")
async def send_test_push(
    payload: PushTestRequest,
    _admin_user: dict = Depends(_require_admin_user),
):
    target_client_ids = []
    if payload.clientId.strip():
        target_client_ids = [payload.clientId.strip()]
    else:
        devices = await list_push_devices(platform="android", push_enabled_only=True)
        target_client_ids = [item.get("client_id", "") for item in devices if item.get("client_id")]

    if not target_client_ids:
        raise HTTPException(status_code=400, detail="没有可用的 clientId")

    try:
        result = await send_notification_to_client_ids(
            client_ids=target_client_ids,
            title=payload.title,
            body=payload.body,
            route=payload.route,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


@push_router.post("/daily-send")
async def send_daily_push(
    limit: int = Query(default=2, ge=1, le=10),
    _admin_user: dict = Depends(_require_admin_user),
):
    try:
        result = await send_daily_highlights_to_registered_devices(limit=limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


@push_router.post("/selected")
async def send_selected_push(
    payload: PushSelectedRequest,
    authorization: str | None = Header(default=None),
    admin_user: dict = Depends(_require_admin_user),
):
    operator = str(admin_user.get("username") or "").strip() or payload.createdBy
    try:
        result = await send_selected_issue_to_registered_devices(
            article_ids=payload.articleIds,
            created_by=operator,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"success": True, "data": result}


@push_router.post("/selected/preview")
async def preview_selected_push(
    payload: PushSelectedRequest,
    authorization: str | None = Header(default=None),
    admin_user: dict = Depends(_require_admin_user),
):
    operator = str(admin_user.get("username") or "").strip() or payload.createdBy
    try:
        issue = await create_selected_issue(payload.articleIds, created_by=operator)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"success": True, "data": issue}


@push_router.post("/selected/wechat-publish")
async def publish_selected_issue_to_wechat_api(
    payload: PushSelectedRequest,
    authorization: str | None = Header(default=None),
    admin_user: dict = Depends(_require_admin_user),
):
    operator = str(admin_user.get("username") or "").strip() or payload.createdBy
    try:
        issue = await create_selected_issue(payload.articleIds, created_by=operator)
        result = await publish_issue_to_wechat(issue, created_by=operator)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"success": True, "data": {"issue": issue, **result}}


@push_router.post("/selected/email-send")
async def send_selected_issue_to_email_api(
    payload: PushSelectedRequest,
    authorization: str | None = Header(default=None),
    admin_user: dict = Depends(_require_admin_user),
):
    operator = str(admin_user.get("username") or "").strip() or payload.createdBy
    try:
        issue = await create_selected_issue(payload.articleIds, created_by=operator)
        result = await send_issue_to_email_subscribers(issue, created_by=operator)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"success": True, "data": result}


@push_router.get("/wechat/publish-status/{publish_id}")
async def get_wechat_publish_status(
    publish_id: str,
    _admin_user: dict = Depends(_require_admin_user),
):
    try:
        result = await sync_wechat_publish_status(publish_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"success": True, "data": result}


@push_router.get("/issues/latest")
async def get_latest_issue():
    issue = await get_latest_push_brief_issue()
    if not issue:
        raise HTTPException(status_code=404, detail="no pushed issue available")
    return {"success": True, "data": issue}


@push_router.get("/issues/{issue_id}")
async def get_issue_by_id(issue_id: str):
    issue = await get_push_brief_issue_by_id(issue_id)
    if not issue:
        raise HTTPException(status_code=404, detail="issue not found")
    return {"success": True, "data": issue}
