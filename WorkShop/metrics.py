"""
Lightweight request-level timing metrics backed by SQLite.

Usage in app.py:
    from metrics import Trace

    trace = Trace("opencode/generate-preview", request_id="abc123")
    with trace.step("health_check"):
        ...
    with trace.step("upstream_generate"):
        ...
    trace.finish()          # or trace.finish(error="some msg")
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

_DB_PATH = os.getenv(
    "WORKSHOP_METRICS_DB",
    str(Path(__file__).resolve().parent / "metrics.db"),
)

_RETENTION_HOURS = int(os.getenv("WORKSHOP_METRICS_RETENTION_HOURS", "72"))

_local = threading.local()

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS requests (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id  TEXT    NOT NULL,
    endpoint    TEXT    NOT NULL,
    started_at  REAL    NOT NULL,
    total_ms    REAL,
    status      TEXT    DEFAULT 'running',
    meta        TEXT    DEFAULT '{}',
    created_at  TEXT    DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_requests_rid     ON requests(request_id);
CREATE INDEX IF NOT EXISTS idx_requests_ep      ON requests(endpoint);
CREATE INDEX IF NOT EXISTS idx_requests_started ON requests(started_at);

CREATE TABLE IF NOT EXISTS steps (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id  TEXT    NOT NULL,
    step_name   TEXT    NOT NULL,
    step_order  INTEGER NOT NULL DEFAULT 0,
    started_at  REAL    NOT NULL,
    duration_ms REAL,
    status      TEXT    DEFAULT 'running',
    meta        TEXT    DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_steps_rid ON steps(request_id);

CREATE TABLE IF NOT EXISTS stream_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id  TEXT    NOT NULL,
    seq         INTEGER NOT NULL DEFAULT 0,
    event_type  TEXT    NOT NULL,
    summary     TEXT    NOT NULL DEFAULT '',
    payload     TEXT    NOT NULL DEFAULT '{}',
    ts          REAL    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stream_events_rid ON stream_events(request_id);
"""


def _get_conn() -> sqlite3.Connection:
    conn: sqlite3.Connection | None = getattr(_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.executescript(_INIT_SQL)
        _local.conn = conn
    return conn


def _now() -> float:
    return time.time()


class StepContext:
    """Returned by Trace.step() context-manager."""

    __slots__ = ("trace", "name", "order", "_t0", "meta")

    def __init__(self, trace: "Trace", name: str, order: int):
        self.trace = trace
        self.name = name
        self.order = order
        self._t0 = 0.0
        self.meta: dict[str, Any] = {}

    def set_meta(self, **kw: Any) -> None:
        self.meta.update(kw)


class Trace:
    """Represents one request's timing lifecycle."""

    def __init__(
        self,
        endpoint: str,
        *,
        request_id: str,
        meta: dict[str, Any] | None = None,
    ):
        self.endpoint = endpoint
        self.request_id = request_id
        self._t0 = time.perf_counter()
        self._started_at = _now()
        self._step_counter = 0
        self._meta = meta or {}
        self._finished = False

        conn = _get_conn()
        conn.execute(
            "INSERT INTO requests (request_id, endpoint, started_at, meta) VALUES (?,?,?,?)",
            (self.request_id, self.endpoint, self._started_at, json.dumps(self._meta, ensure_ascii=False, default=str)),
        )
        conn.commit()

    @contextmanager
    def step(self, name: str, **extra_meta: Any) -> Generator[StepContext, None, None]:
        self._step_counter += 1
        ctx = StepContext(self, name, self._step_counter)
        ctx.meta.update(extra_meta)
        t0 = time.perf_counter()
        ctx._t0 = _now()
        status = "ok"
        try:
            yield ctx
        except Exception as exc:
            status = "error"
            ctx.meta["error"] = str(exc)
            raise
        finally:
            duration_ms = round((time.perf_counter() - t0) * 1000, 2)
            conn = _get_conn()
            conn.execute(
                "INSERT INTO steps (request_id, step_name, step_order, started_at, duration_ms, status, meta) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    self.request_id,
                    name,
                    ctx.order,
                    ctx._t0,
                    duration_ms,
                    status,
                    json.dumps(ctx.meta, ensure_ascii=False, default=str),
                ),
            )
            conn.commit()

    def record_step(self, name: str, duration_ms: float, *, status: str = "ok", **meta: Any) -> None:
        """Record a step after the fact (when context-manager is inconvenient)."""
        self._step_counter += 1
        conn = _get_conn()
        conn.execute(
            "INSERT INTO steps (request_id, step_name, step_order, started_at, duration_ms, status, meta) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                self.request_id,
                name,
                self._step_counter,
                _now(),
                round(duration_ms, 2),
                status,
                json.dumps(meta, ensure_ascii=False, default=str),
            ),
        )
        conn.commit()

    def finish(self, *, error: str | None = None) -> None:
        if self._finished:
            return
        self._finished = True
        total_ms = round((time.perf_counter() - self._t0) * 1000, 2)
        status = "error" if error else "ok"
        if error:
            self._meta["error"] = error

        conn = _get_conn()
        conn.execute(
            "UPDATE requests SET total_ms=?, status=?, meta=? WHERE request_id=?",
            (
                total_ms,
                status,
                json.dumps(self._meta, ensure_ascii=False, default=str),
                self.request_id,
            ),
        )
        conn.commit()


# --------------- query helpers (used by monitor.py) ---------------

def query_recent_requests(
    limit: int = 50,
    endpoint: str | None = None,
    offset: int = 0,
) -> list[dict[str, Any]]:
    conn = _get_conn()
    clauses = []
    params: list[Any] = []
    if endpoint:
        clauses.append("endpoint = ?")
        params.append(endpoint)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(
        f"SELECT * FROM requests {where} ORDER BY started_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()
    return [dict(r) for r in rows]


def query_steps_for_request(request_id: str) -> list[dict[str, Any]]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM steps WHERE request_id=? ORDER BY step_order",
        (request_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def query_request_with_steps(request_id: str) -> dict[str, Any] | None:
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM requests WHERE request_id=?", (request_id,)
    ).fetchone()
    if not row:
        return None
    result = dict(row)
    result["steps"] = query_steps_for_request(request_id)
    return result


def query_stats(hours: int = 24) -> dict[str, Any]:
    conn = _get_conn()
    cutoff = _now() - hours * 3600
    rows = conn.execute(
        """
        SELECT endpoint,
               COUNT(*)                       AS cnt,
               ROUND(AVG(total_ms), 1)        AS avg_ms,
               ROUND(MIN(total_ms), 1)        AS min_ms,
               ROUND(MAX(total_ms), 1)        AS max_ms,
               SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors
        FROM requests
        WHERE started_at >= ? AND total_ms IS NOT NULL
        GROUP BY endpoint
        """,
        (cutoff,),
    ).fetchall()
    return {
        "hours": hours,
        "endpoints": [dict(r) for r in rows],
    }


def query_step_stats(endpoint: str, hours: int = 24) -> list[dict[str, Any]]:
    conn = _get_conn()
    cutoff = _now() - hours * 3600
    rows = conn.execute(
        """
        SELECT s.step_name,
               COUNT(*)                        AS cnt,
               ROUND(AVG(s.duration_ms), 1)    AS avg_ms,
               ROUND(MIN(s.duration_ms), 1)    AS min_ms,
               ROUND(MAX(s.duration_ms), 1)    AS max_ms
        FROM steps s
        JOIN requests r ON r.request_id = s.request_id
        WHERE r.endpoint = ? AND r.started_at >= ? AND s.duration_ms IS NOT NULL
        GROUP BY s.step_name
        ORDER BY MIN(s.step_order)
        """,
        (endpoint, cutoff),
    ).fetchall()
    return [dict(r) for r in rows]


def record_stream_event(
    request_id: str,
    seq: int,
    event_type: str,
    summary: str,
    payload: dict[str, Any] | str,
) -> None:
    conn = _get_conn()
    raw = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, default=str)
    conn.execute(
        "INSERT INTO stream_events (request_id, seq, event_type, summary, payload, ts) VALUES (?,?,?,?,?,?)",
        (request_id, seq, event_type, summary, raw, _now()),
    )
    conn.commit()


def query_stream_events(request_id: str) -> list[dict[str, Any]]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM stream_events WHERE request_id=? ORDER BY seq",
        (request_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def purge_old(hours: int | None = None) -> int:
    h = hours if hours is not None else _RETENTION_HOURS
    cutoff = _now() - h * 3600
    conn = _get_conn()
    conn.execute("DELETE FROM stream_events WHERE request_id IN (SELECT request_id FROM requests WHERE started_at < ?)", (cutoff,))
    conn.execute("DELETE FROM steps WHERE request_id IN (SELECT request_id FROM requests WHERE started_at < ?)", (cutoff,))
    cur = conn.execute("DELETE FROM requests WHERE started_at < ?", (cutoff,))
    conn.commit()
    return cur.rowcount
