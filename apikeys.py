from __future__ import annotations
#ssr-api/apikeys.py
import secrets
import time
from fastapi import HTTPException

API_KEY_HEADER = "X-API-Key"


async def create_api_key(*, conn, client_id: int, name: str) -> str:
    raw = "ssr_" + secrets.token_urlsafe(32)
    now = int(time.time())

    await conn.execute(
        """
        INSERT INTO ssr_api_keys (client_id, key, name, created_at)
        VALUES ($1, $2, $3, $4)
        """,
        int(client_id),
        raw,
        name,
        now,
    )
    return raw


async def delete_api_key(*, conn, key: str) -> int:
    res = await conn.execute("DELETE FROM ssr_api_keys WHERE key=$1", key)
    try:
        return int(str(res).split()[-1])
    except Exception:
        return 0


async def verify_api_key(*, conn, x_api_key: str | None) -> dict:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API key")

    row = await conn.fetchrow(
        "SELECT id, client_id, name, key FROM ssr_api_keys WHERE key=$1",
        x_api_key,
    )
    if not row:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return {
        "id": int(row["id"]),
        "client_id": int(row["client_id"]),
        "name": row["name"],
        "key": row["key"],
    }
