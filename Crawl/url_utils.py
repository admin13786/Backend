from typing import Iterable, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "mkt_tok",
    "si",
    "spm",
    "spm_id_from",
}

TRACKING_QUERY_PREFIXES: Tuple[str, ...] = (
    "utm_",
)


def _is_tracking_query_key(key: str) -> bool:
    k = (key or "").strip().lower()
    if not k:
        return False
    if k in TRACKING_QUERY_KEYS:
        return True
    return any(k.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES)


def _normalize_netloc(scheme: str, netloc: str) -> str:
    raw = (netloc or "").strip()
    if not raw:
        return ""

    if "@" in raw:
        _, raw = raw.rsplit("@", 1)

    host, sep, port = raw.partition(":")
    host = host.lower()
    if not sep:
        return host

    if (scheme == "http" and port == "80") or (scheme == "https" and port == "443"):
        return host
    return f"{host}:{port}"


def _normalize_path(path: str) -> str:
    normalized = (path or "").strip() or "/"
    if normalized != "/" and normalized.endswith("/"):
        normalized = normalized.rstrip("/")
    return normalized


def _normalize_query_items(items: Iterable[Tuple[str, str]]) -> str:
    kept = []
    for key, value in items:
        if _is_tracking_query_key(key):
            continue
        kept.append((key, value))
    if not kept:
        return ""
    kept.sort()
    return urlencode(kept, doseq=True)


def normalize_article_url(url: str) -> str:
    candidate = (url or "").strip()
    if not candidate:
        return ""

    try:
        parts = urlsplit(candidate)
    except Exception:
        return candidate

    scheme = (parts.scheme or "").lower()
    netloc = _normalize_netloc(scheme, parts.netloc)
    path = _normalize_path(parts.path)
    query = _normalize_query_items(parse_qsl(parts.query, keep_blank_values=True))

    normalized = urlunsplit((scheme, netloc, path, query, ""))
    return normalized or candidate
