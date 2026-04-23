import asyncio
import logging
import os
import re
import smtplib
import ssl
from email.message import EmailMessage
from html import escape
from typing import Dict, List, Optional
from urllib.parse import urlencode

from db import (
    create_email_publish_record,
    list_email_subscribers,
    touch_email_subscriber_sent_at,
)
from env_loader import load_crawl_env


load_crawl_env()

logger = logging.getLogger("email_service")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

EMAIL_PUSH_ENABLE = str(os.getenv("EMAIL_PUSH_ENABLE", "0")).strip().lower() not in {
    "",
    "0",
    "false",
    "off",
    "no",
}
EMAIL_SMTP_HOST = str(os.getenv("EMAIL_SMTP_HOST", "") or "").strip()
EMAIL_SMTP_PORT = int(str(os.getenv("EMAIL_SMTP_PORT", "465") or "465").strip() or "465")
EMAIL_SMTP_USERNAME = str(os.getenv("EMAIL_SMTP_USERNAME", "") or "").strip()
EMAIL_SMTP_PASSWORD = str(os.getenv("EMAIL_SMTP_PASSWORD", "") or "").strip()
EMAIL_SMTP_USE_SSL = str(os.getenv("EMAIL_SMTP_USE_SSL", "1") or "1").strip().lower() not in {
    "0",
    "false",
    "off",
    "no",
}
EMAIL_SMTP_STARTTLS = str(os.getenv("EMAIL_SMTP_STARTTLS", "0") or "0").strip().lower() not in {
    "0",
    "false",
    "off",
    "no",
}
EMAIL_SMTP_TIMEOUT = float(str(os.getenv("EMAIL_SMTP_TIMEOUT", "20") or "20").strip() or "20")
EMAIL_FROM_ADDRESS = str(os.getenv("EMAIL_FROM_ADDRESS", "") or "").strip()
EMAIL_FROM_NAME = str(os.getenv("EMAIL_FROM_NAME", "AI趣闻萃取") or "").strip() or "AI趣闻萃取"
EMAIL_REPLY_TO = str(os.getenv("EMAIL_REPLY_TO", "") or "").strip()
EMAIL_SUBJECT_PREFIX = str(os.getenv("EMAIL_SUBJECT_PREFIX", "") or "").strip()
EMAIL_STATIC_SUBSCRIBERS = str(os.getenv("EMAIL_STATIC_SUBSCRIBERS", "") or "").strip()
EMAIL_UNSUBSCRIBE_BASE_URL = str(os.getenv("EMAIL_UNSUBSCRIBE_BASE_URL", "") or "").strip()
EMAIL_MAX_FAILURE_DETAILS = int(
    str(os.getenv("EMAIL_MAX_FAILURE_DETAILS", "20") or "20").strip() or "20"
)


def _clip_text(value: str, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _normalize_email_address(email: str) -> str:
    normalized = str(email or "").strip().lower()
    if not normalized or not EMAIL_RE.fullmatch(normalized):
        raise ValueError("invalid email")
    return normalized


def _normalize_text_list(value) -> List[str]:
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    text = str(value or "").strip()
    return [text] if text else []


def _safe_cover_url(value: str) -> str:
    url = str(value or "").strip()
    return url if url.startswith("http://") or url.startswith("https://") else ""


def _parse_static_subscribers() -> List[Dict]:
    if not EMAIL_STATIC_SUBSCRIBERS:
        return []

    normalized: List[Dict] = []
    seen = set()
    raw_items = re.split(r"[\s,;]+", EMAIL_STATIC_SUBSCRIBERS)
    for raw in raw_items:
        value = str(raw or "").strip()
        if not value:
            continue
        try:
            email = _normalize_email_address(value)
        except ValueError:
            logger.warning("ignore invalid static email subscriber: %s", value)
            continue
        if email in seen:
            continue
        seen.add(email)
        normalized.append(
            {
                "email": email,
                "displayName": "",
                "source": "env",
                "unsubscribeToken": "",
            }
        )
    return normalized


async def resolve_email_recipients() -> List[Dict]:
    recipients: List[Dict] = []
    seen = set()

    for item in await list_email_subscribers(active_only=True):
        email = str(item.get("email") or "").strip().lower()
        if not email or email in seen:
            continue
        seen.add(email)
        recipients.append(item)

    for item in _parse_static_subscribers():
        email = item["email"]
        if email in seen:
            continue
        seen.add(email)
        recipients.append(item)

    return recipients


def is_email_configured() -> bool:
    return EMAIL_PUSH_ENABLE and bool(EMAIL_SMTP_HOST) and bool(EMAIL_FROM_ADDRESS)


async def get_email_config_status() -> Dict:
    db_subscribers = await list_email_subscribers(active_only=True)
    static_subscribers = _parse_static_subscribers()
    recipients = await resolve_email_recipients()
    return {
        "enabled": EMAIL_PUSH_ENABLE,
        "configured": is_email_configured(),
        "hasSmtpHost": bool(EMAIL_SMTP_HOST),
        "hasFromAddress": bool(EMAIL_FROM_ADDRESS),
        "hasUsername": bool(EMAIL_SMTP_USERNAME),
        "subscriberCount": len(recipients),
        "dbSubscriberCount": len(db_subscribers),
        "staticSubscriberCount": len(static_subscribers),
    }


def _format_subject(issue: Dict) -> str:
    issue_title = str(issue.get("title") or "").strip()
    if not issue_title:
        issue_date = str(issue.get("date") or "").strip()
        issue_title = f"{issue_date} AI趣闻萃取".strip() or "AI趣闻萃取"
    subject = f"{EMAIL_SUBJECT_PREFIX}{issue_title}".strip()
    return _clip_text(subject, 120)


def _build_unsubscribe_link(recipient: Dict) -> str:
    if not EMAIL_UNSUBSCRIBE_BASE_URL:
        return ""
    token = str(recipient.get("unsubscribeToken") or "").strip()
    if not token:
        return ""
    separator = "&" if "?" in EMAIL_UNSUBSCRIBE_BASE_URL else "?"
    return f"{EMAIL_UNSUBSCRIBE_BASE_URL}{separator}{urlencode({'token': token})}"


def _render_item_html(item: Dict) -> str:
    headline = escape(str(item.get("headline") or "").strip() or "AI 热点")
    warning = escape(str(item.get("warning") or "").strip())
    source = escape(str(item.get("source") or "").strip() or "AI News")
    cover_url = _safe_cover_url(item.get("coverImage") or "")
    article_url = str(item.get("articleUrl") or "").strip()
    article_href = article_url if article_url.startswith(("http://", "https://")) else ""
    paragraphs = _normalize_text_list(item.get("expandedBody"))
    paragraphs_html = "".join(
        f"<p style=\"margin:0 0 10px;line-height:1.7;color:#314158;font-size:14px;\">{escape(paragraph)}</p>"
        for paragraph in paragraphs
    )
    action_html = (
        f"<a href=\"{escape(article_href)}\" "
        "style=\"display:inline-block;margin-top:8px;padding:10px 14px;background:#1d4ed8;color:#ffffff;"
        "text-decoration:none;border-radius:999px;font-size:13px;font-weight:600;\">查看原文</a>"
        if article_href
        else ""
    )
    image_html = (
        f"<img src=\"{escape(cover_url)}\" alt=\"{headline}\" "
        "style=\"width:100%;max-height:220px;object-fit:cover;border-radius:16px;margin:0 0 14px;display:block;\" />"
        if cover_url
        else ""
    )

    return (
        "<article style=\"background:#ffffff;border:1px solid #e5e7eb;border-radius:20px;"
        "padding:18px 18px 16px;margin:0 0 18px;box-shadow:0 10px 30px rgba(15,23,42,0.06);\">"
        f"{image_html}"
        f"<div style=\"font-size:12px;color:#6b7280;margin:0 0 8px;font-weight:600;letter-spacing:0.02em;\">{source}</div>"
        f"<h2 style=\"margin:0 0 10px;font-size:22px;line-height:1.35;color:#0f172a;\">{headline}</h2>"
        f"<p style=\"margin:0 0 12px;color:#b45309;background:#fff7ed;border:1px solid #fed7aa;"
        f"padding:10px 12px;border-radius:14px;font-size:13px;line-height:1.6;\">{warning}</p>"
        f"{paragraphs_html}"
        f"{action_html}"
        "</article>"
    )


def render_issue_email_html(issue: Dict, recipient: Optional[Dict] = None) -> str:
    title = escape(str(issue.get("title") or "").strip() or "AI趣闻萃取")
    subtitle = escape(str(issue.get("subtitle") or "").strip())
    footer = escape(str(issue.get("footer") or "").strip())
    issue_date = escape(str(issue.get("date") or "").strip())
    items_html = "".join(_render_item_html(item) for item in (issue.get("items") or []))
    unsubscribe_link = _build_unsubscribe_link(recipient or {})
    unsubscribe_html = (
        f"<p style=\"margin:16px 0 0;font-size:12px;color:#6b7280;\">如不再接收，"
        f"<a href=\"{escape(unsubscribe_link)}\" style=\"color:#2563eb;\">点此退订</a></p>"
        if unsubscribe_link
        else ""
    )

    return (
        "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\" />"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />"
        f"<title>{title}</title></head>"
        "<body style=\"margin:0;padding:0;background:#f4f7fb;color:#111827;font-family:"
        "-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;\">"
        "<div style=\"max-width:720px;margin:0 auto;padding:28px 16px 40px;\">"
        "<div style=\"background:linear-gradient(135deg,#0f172a,#1d4ed8);border-radius:28px;padding:28px 24px;"
        "color:#ffffff;box-shadow:0 18px 40px rgba(15,23,42,0.22);margin:0 0 18px;\">"
        f"<div style=\"font-size:13px;letter-spacing:0.08em;text-transform:uppercase;opacity:0.82;\">{issue_date}</div>"
        f"<h1 style=\"margin:10px 0 12px;font-size:30px;line-height:1.2;\">{title}</h1>"
        f"<p style=\"margin:0;font-size:15px;line-height:1.7;opacity:0.92;\">{subtitle}</p>"
        "</div>"
        f"{items_html}"
        "<div style=\"background:#ffffff;border:1px solid #e5e7eb;border-radius:20px;padding:18px 20px;"
        "color:#475569;font-size:13px;line-height:1.8;\">"
        f"<div>{footer}</div>"
        f"{unsubscribe_html}"
        "</div>"
        "</div></body></html>"
    )


def render_issue_email_text(issue: Dict, recipient: Optional[Dict] = None) -> str:
    lines = [
        str(issue.get("title") or "").strip() or "AI趣闻萃取",
        str(issue.get("subtitle") or "").strip(),
        "",
    ]
    for index, item in enumerate(issue.get("items") or [], 1):
        lines.append(f"{index}. {str(item.get('headline') or '').strip()}")
        warning = str(item.get("warning") or "").strip()
        if warning:
            lines.append(f"   {warning}")
        for paragraph in _normalize_text_list(item.get("expandedBody")):
            lines.append(f"   {paragraph}")
        article_url = str(item.get("articleUrl") or "").strip()
        if article_url:
            lines.append(f"   原文: {article_url}")
        lines.append("")

    footer = str(issue.get("footer") or "").strip()
    if footer:
        lines.append(footer)
    unsubscribe_link = _build_unsubscribe_link(recipient or {})
    if unsubscribe_link:
        lines.append(f"退订: {unsubscribe_link}")
    return "\n".join(line for line in lines if line is not None)


def _build_message(recipient: Dict, subject: str, html: str, text: str) -> EmailMessage:
    email_address = _normalize_email_address(recipient.get("email") or "")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{EMAIL_FROM_NAME} <{EMAIL_FROM_ADDRESS}>" if EMAIL_FROM_NAME else EMAIL_FROM_ADDRESS
    msg["To"] = email_address
    if EMAIL_REPLY_TO:
        msg["Reply-To"] = EMAIL_REPLY_TO
    msg.set_content(text)
    msg.add_alternative(html, subtype="html")
    return msg


def _send_message_sync(message: EmailMessage) -> None:
    context = ssl.create_default_context()
    if EMAIL_SMTP_USE_SSL:
        with smtplib.SMTP_SSL(
            EMAIL_SMTP_HOST,
            EMAIL_SMTP_PORT,
            timeout=EMAIL_SMTP_TIMEOUT,
            context=context,
        ) as server:
            if EMAIL_SMTP_USERNAME:
                server.login(EMAIL_SMTP_USERNAME, EMAIL_SMTP_PASSWORD)
            server.send_message(message)
        return

    with smtplib.SMTP(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, timeout=EMAIL_SMTP_TIMEOUT) as server:
        server.ehlo()
        if EMAIL_SMTP_STARTTLS:
            server.starttls(context=context)
            server.ehlo()
        if EMAIL_SMTP_USERNAME:
            server.login(EMAIL_SMTP_USERNAME, EMAIL_SMTP_PASSWORD)
        server.send_message(message)


async def send_issue_to_email_subscribers(issue: Dict, *, created_by: str = "") -> Dict:
    if not is_email_configured():
        raise RuntimeError("email channel is not configured")

    recipients = await resolve_email_recipients()
    if not recipients:
        logger.info("selected issue email skipped: no active subscribers")
        return {
            "success": True,
            "sent": 0,
            "failedCount": 0,
            "reason": "no_subscribers",
            "issue": issue,
        }

    subject = _format_subject(issue)
    successes: List[str] = []
    failures: List[Dict] = []

    for recipient in recipients:
        html = render_issue_email_html(issue, recipient=recipient)
        text = render_issue_email_text(issue, recipient=recipient)
        try:
            message = _build_message(recipient, subject, html, text)
            await asyncio.to_thread(_send_message_sync, message)
            successes.append(str(recipient.get("email") or "").strip().lower())
            if recipient.get("source") != "env":
                await touch_email_subscriber_sent_at(recipient.get("email") or "")
        except Exception as exc:
            logger.warning("issue email send failed recipient=%s error=%s", recipient.get("email"), exc)
            if len(failures) < EMAIL_MAX_FAILURE_DETAILS:
                failures.append(
                    {
                        "email": str(recipient.get("email") or "").strip().lower(),
                        "error": str(exc),
                    }
                )

    sent_count = len(successes)
    failed_count = max(0, len(recipients) - sent_count)
    status = "sent" if failed_count == 0 else "partial_failed" if sent_count > 0 else "failed"
    record = await create_email_publish_record(
        issue_id=str(issue.get("id") or "").strip(),
        subject=subject,
        status=status,
        sent_count=sent_count,
        failed_count=failed_count,
        created_by=created_by,
        raw={
            "totalRecipients": len(recipients),
            "successes": successes,
            "failures": failures,
        },
    )

    return {
        "success": sent_count > 0 and failed_count == 0,
        "issue": issue,
        "subject": subject,
        "sent": sent_count,
        "failedCount": failed_count,
        "totalRecipients": len(recipients),
        "status": status,
        "record": record,
        "failures": failures,
    }
