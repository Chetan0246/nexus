"""ASGI app factory, lifespan, CORS, and health telemetry."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from nexus.api.routers import auth, docs, ws
from nexus.api.ws import ConnectionHub
from nexus.config import get_settings
from nexus.db import init_db


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await init_db()
    yield
    from nexus.db import engine

    await engine.dispose()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Nexus API",
        version="1.0.0",
        description="Autonomous Web Ingestion & Realtime Intelligence API",
        lifespan=lifespan,
        docs_url="/swagger" if settings.app_env != "prod" else None,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(auth.router)
    app.include_router(docs.router)
    app.include_router(ws.router)

    app.state.hub = ConnectionHub()
    start_time = time.monotonic()

    @app.get("/health", tags=["meta"])
    async def health() -> dict[str, Any]:
        hub = getattr(app.state, "hub", None)
        active_rooms = len(hub.room_count()) if hub else 0
        return {
            "status": "ok",
            "app": settings.app_name,
            "env": settings.app_env,
            "version": "1.0.0",
            "uptime_seconds": round(time.monotonic() - start_time, 2),
            "active_ws_rooms": active_rooms,
        }

    return app


app = create_app()
