from __future__ import annotations
#ssr-api/db.py
import asyncpg
from fastapi import HTTPException
from settings import SSR_DB_DSN

_pool: asyncpg.Pool | None = None


async def init_db_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool

    if not SSR_DB_DSN:
        raise RuntimeError("SSR_DB_DSN is not configured")

    _pool = await asyncpg.create_pool(
        dsn=SSR_DB_DSN,
        min_size=1,
        max_size=5,
        command_timeout=60,
    )
    return _pool


async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def get_db():
    """
    Dependency compatible with FastAPI Depends.
    Returns asyncpg pool (like your other services).
    """
    pool = await init_db_pool()
    return pool
