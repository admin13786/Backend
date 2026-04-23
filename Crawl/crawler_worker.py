"""
Crawl worker entrypoint.

- Initializes the database
- Starts the scheduled crawl job
"""

import os
import sys
import uvicorn

from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI

from crawl_log_sessions import rotate_crawl_log_if_nonempty
from db import close_pool, init_db
from env_loader import load_crawl_env
from news_api import scheduled_crawl
from tz_display import TZ_SHANGHAI, format_shanghai_local

load_crawl_env()

LOG_PATH = os.getenv("CRAWL_LOG_PATH", "/app/logs/crawl_log.txt")


class TeeWriter:
    """Mirror stdout/stderr to the crawl log file for monitoring."""

    def __init__(self, original, log_path: str):
        self.original = original
        self.log_path = log_path

    def write(self, msg: str):
        try:
            self.original.write(msg)
        except UnicodeEncodeError:
            encoding = self.original.encoding or "utf-8"
            safe = msg.encode(encoding, errors="replace").decode(encoding, errors="replace")
            self.original.write(safe)
        if msg.strip():
            try:
                with open(self.log_path, "a", encoding="utf-8") as f:
                    f.write(msg)
                    if not msg.endswith("\n"):
                        f.write("\n")
            except Exception:
                pass

    def flush(self):
        self.original.flush()

    def isatty(self):
        return self.original.isatty()


os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
rotate_crawl_log_if_nonempty(LOG_PATH)
sys.stdout = TeeWriter(sys.stdout, LOG_PATH)
sys.stderr = TeeWriter(sys.stderr, LOG_PATH)

print(f"\n{'=' * 60}")
print(f"Worker start time: {format_shanghai_local()}")
print(f"Live log file: {LOG_PATH}")
print(f"{'=' * 60}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()

    scheduler = AsyncIOScheduler(timezone=TZ_SHANGHAI)
    scheduler.add_job(
        scheduled_crawl,
        CronTrigger(hour=9, minute=0, timezone=TZ_SHANGHAI),
        args=["business"],
        id="biz_crawl",
        name="business_daily_crawl",
    )
    scheduler.start()
    print("[SCHED] Worker daily crawl started: 09:00 Asia/Shanghai business")

    try:
        yield
    finally:
        scheduler.shutdown()
        await close_pool()


app = FastAPI(title="Crawl Worker", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "crawler_worker"}


if __name__ == "__main__":
    port = int(os.getenv("WORKER_PORT", "6600"))
    uvicorn.run(app, host="0.0.0.0", port=port)
