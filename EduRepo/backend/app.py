from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from edu_api import edu_router

app = FastAPI(title="EduRepo Demo")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(edu_router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "edurepo", "version": "0.1.0"}

