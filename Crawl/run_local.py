"""
Local entrypoint for the AI News Agent.

- Uses direct RSS/API collection instead of browser automation
- Writes logs to `logs/crawl_log.txt`
- Starts a daily scheduled crawl at 09:00 (no crawl on process startup)
"""

import logging
import os
import sys
import uvicorn

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


# Ensure local module imports resolve from this directory first.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "crawl_log.txt")
os.makedirs(os.path.dirname(log_file), exist_ok=True)

from crawl_log_sessions import rotate_crawl_log_if_nonempty

rotate_crawl_log_if_nonempty(log_file)


# Mirror logs to stdout and the crawl log file.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8", mode="a"),
    ],
)


class TeeWriter:
    """Mirror print output to both the console and the crawl log file."""

    def __init__(self, original, log_path):
        self.original = original
        self.log_path = log_path

    def write(self, msg):
        try:
            self.original.write(msg)
        except UnicodeEncodeError:
            encoding = self.original.encoding or "gbk"
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


sys.stdout = TeeWriter(sys.stdout, log_file)
sys.stderr = TeeWriter(sys.stderr, log_file)

from analytics_api import analytics_router
from news_api import news_router, scheduled_crawl
from push_api import push_router
from push_service import is_push_configured, send_daily_highlights_to_registered_devices
from rank_api import rank_router
from db import close_pool, init_db


@asynccontextmanager
async def lifespan(app):
    # Initialize the database connection on startup.
    try:
        await init_db()
    except Exception as e:
        print(f"[FATAL] Database init failed: {e}")
        raise

    # Start the daily crawl scheduler.
    scheduler = None
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            scheduled_crawl,
            CronTrigger(hour=9, minute=0),
            args=["business"],
            id="biz_crawl",
            name="business_daily_crawl",
        )
        if is_push_configured():
            scheduler.add_job(
                send_daily_highlights_to_registered_devices,
                CronTrigger(hour=9, minute=5),
                kwargs={"limit": 2},
                id="daily_push",
                name="daily_highlights_push",
            )
        scheduler.start()
        print("[SCHED] Daily crawl started: 09:00 business")
        if is_push_configured():
            print("[SCHED] Daily push started: 09:05 highlights")
        else:
            print("[WARN] uniPush/Getui config missing; daily push will fail until env vars are set")
    except ImportError:
        print("[WARN] apscheduler is not installed; scheduled crawl is unavailable (pip install apscheduler)")
    except Exception as e:
        print(f"[WARN] Failed to start scheduled crawl: {e}")

    yield

    # Shut down background resources gracefully.
    if scheduler:
        scheduler.shutdown()
    await close_pool()


app = FastAPI(title="AI News Agent - Local Mode", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Support both legacy and current API prefixes.
app.include_router(news_router)
app.include_router(news_router, prefix="/api")
app.include_router(analytics_router)
app.include_router(push_router)
app.include_router(rank_router)

# Serve local static assets when present.
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/health")
async def health():
    return {"status": "ok", "mode": "local", "headless": False}


if __name__ == "__main__":
    print("=" * 50)
    print("AI News Agent - Local Mode")
    print("=" * 50)
    print("API docs: http://localhost:8000/docs")
    print("Playwright disabled; using direct RSS/API collection")
    print(f"Log file: {log_file}")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=8000)
