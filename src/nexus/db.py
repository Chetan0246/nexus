"""Async database connectivity and session management with SQLAlchemy 2.0."""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from nexus.config import get_settings


class Base(DeclarativeBase):
    pass


def build_engine(url: str | None = None) -> AsyncEngine:
    db_url = url or get_settings().database_url
    is_sqlite = db_url.startswith("sqlite")
    kwargs = {"echo": False, "future": True}
    if not is_sqlite:
        kwargs.update({"pool_size": 20, "max_overflow": 10})
    return create_async_engine(db_url, **kwargs)


engine: AsyncEngine = build_engine()
SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine,
    expire_on_commit=False,
)
AsyncSessionLocal = SessionLocal


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
