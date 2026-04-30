"""统一使用东八区（Asia/Shanghai）展示与批次时间戳；数据库存储仍按原逻辑（时间戳视为 UTC 再换算）。"""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")


def now_shanghai() -> datetime:
    return datetime.now(TZ_SHANGHAI)


def batch_ts_suffix() -> str:
    """biz_/personal_ 批次 ID 内嵌的时间后缀。"""
    return now_shanghai().strftime("%Y%m%d_%H%M%S")


def format_shanghai_local(dt: datetime | None = None) -> str:
    """格式化为本地可读时间（东八区）。"""
    if dt is None:
        dt = now_shanghai()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")


def naive_utc_from_timestamp(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def db_value_to_shanghai_display(val) -> str:
    """将 SQLite / API 中的时间字符串按 UTC 读入，再格式化为东八区。"""
    if val is None or val == "":
        return ""
    s = str(val).strip()
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        elif "T" in s:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            if len(s) >= 19:
                s19 = s[:19]
            else:
                s19 = s
            dt = datetime.strptime(s19, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return s
