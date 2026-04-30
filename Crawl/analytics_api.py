import logging

from fastapi import APIRouter
from pydantic import BaseModel


analytics_router = APIRouter(prefix="/api/analytics", tags=["analytics"])


class PageViewPayload(BaseModel):
    page_key: str = ""
    guest_id: str = ""


@analytics_router.post("/page-view")
async def page_view(payload: PageViewPayload):
    logging.info(
        "[analytics] page_view page_key=%s guest_id=%s",
        (payload.page_key or "").strip(),
        (payload.guest_id or "").strip(),
    )
    return {"success": True}
