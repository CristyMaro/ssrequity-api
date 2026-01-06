from __future__ import annotations
#ssr-api/ssr.py
import io
import time
import uuid
import csv

from fastapi import APIRouter, Depends, File, UploadFile, Header, HTTPException
from db import get_db
from apikeys import verify_api_key, create_api_key, delete_api_key
from settings import SSR_EQUITY_ADMIN_TOKEN, MAX_UPLOAD_BYTES

router = APIRouter()


# -------------------------
# Admin auth
# -------------------------
def require_admin(x_admin_token: str | None = Header(None)):
    if not x_admin_token or x_admin_token != SSR_EQUITY_ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="admin token invalid")
    return True


@router.post("/admin/api-keys")
async def admin_create_api_key(
    payload: dict,
    _=Depends(require_admin),
    db=Depends(get_db),
):
    client_id = payload.get("client_id")
    name = (payload.get("name") or "").strip()
    if client_id is None:
        raise HTTPException(status_code=400, detail="client_id is required")
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    async with db.acquire() as conn:
        key = await create_api_key(conn=conn, client_id=int(client_id), name=name)
    return {"key": key, "name": name, "client_id": int(client_id)}


@router.delete("/admin/api-keys/{key}")
async def admin_delete_api_key(
    key: str,
    _=Depends(require_admin),
    db=Depends(get_db),
):
    async with db.acquire() as conn:
        deleted = await delete_api_key(conn=conn, key=key)
    return {"deleted": deleted}


# -------------------------
# Client import
# -------------------------

@router.post("/client/ssr/import")
async def client_ssr_import(
    file: UploadFile = File(...),
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    db=Depends(get_db),
):
    # Read bytes
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File too large (max {MAX_UPLOAD_BYTES} bytes)")

    # Parse CSV (minimal, flexible)
    # We store rows into ssr_positions_raw with your existing schema.
    # Required fields we can infer:
    # - client_id from API key
    # - upload_batch_id uuid
    # - source_filename, source_row_no
    # - as_of_date: try to read from column 'as_of_date' or 'date', else error.
    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=400, detail="CSV has no rows")

    async with db.acquire() as conn:
        # verify api key
        auth = await verify_api_key(conn=conn, x_api_key=x_api_key)
        client_id = int(auth["client_id"])
        api_key_id = int(auth["id"])  # not used now, but useful later

        batch_id = uuid.uuid4()
        now_ts = int(time.time())

        # Determine as_of_date (strict minimal rule)
        # We expect a column in the CSV named as_of_date or date.
        # Format accepted: YYYY-MM-DD.
        def pick_as_of(d: dict) -> str:
            v = (d.get("as_of_date") or d.get("date") or "").strip()
            if not v:
                raise HTTPException(status_code=400, detail="Missing as_of_date/date column in CSV")
            return v

        as_of_date = pick_as_of(rows[0])

        # 1) insert upload log (your current table)
        await conn.execute(
            """
            INSERT INTO ssr_position_uploads (client_id, upload_batch_id, file_name, uploaded_at, status, total_rows, details)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            client_id,
            batch_id,
            (file.filename or "positions.csv"),
            now_ts,
            "RECEIVED",
            len(rows),
            None,
        )

        # 2) insert raw rows (minimal mapping: store what we can)
        # Your table requires: client_id, as_of_date, upload_batch_id, activity_type, ticker, instrument_type, country, quantity, notional
        # We'll attempt to map common column names; if missing mandatory, fail with 400.
        def req(d: dict, *keys: str) -> str:
            for k in keys:
                v = (d.get(k) or "").strip()
                if v:
                    return v
            raise HTTPException(status_code=400, detail=f"Missing required column: one of {keys}")

        def num(d: dict, *keys: str) -> str:
            v = req(d, *keys)
            return v.replace(",", "")  # tolerate 1,234.56

        inserts = []
        for i, r in enumerate(rows, start=2):  # header is line 1
            ticker = req(r, "ticker", "symbol")
            instrument_type = req(r, "instrument_type", "type")
            country = req(r, "country", "country_code")
            quantity = num(r, "quantity", "qty")
            notional = num(r, "notional", "notional_value", "value")

            inserts.append(
                (
                    client_id,
                    as_of_date,
                    batch_id,
                    file.filename or "positions.csv",
                    i,
                    None,                 # entity_id
                    (r.get("fund_id") or None),
                    (r.get("portfolio_id") or None),
                    "non_management",     # activity_type default matches your table default
                    ticker,
                    (r.get("isin") or None),
                    instrument_type,
                    country,
                    (r.get("type_of_delivery") or None),
                    quantity,
                    notional,
                    (r.get("price") or None),
                    (r.get("currency") or None),
                    (r.get("underlying_ticker") or None),
                    (r.get("delta") or None),
                )
            )

        await conn.executemany(
            """
            INSERT INTO ssr_positions_raw (
              client_id, as_of_date, upload_batch_id, source_filename, source_row_no,
              entity_id, fund_id, portfolio_id, activity_type,
              ticker, isin, instrument_type, country, type_of_delivery,
              quantity, notional, price, currency, underlying_ticker, delta
            )
            VALUES (
              $1, $2::date, $3, $4, $5,
              $6, $7, $8, $9::ssr_activity_type,
              $10, $11, $12, $13, $14,
              $15::numeric, $16::numeric, $17::numeric, $18, $19, $20::numeric
            )
            """,
            inserts,
        )

        # 3) update upload status to STORED
        await conn.execute(
            """
            UPDATE ssr_position_uploads
            SET status='STORED'
            WHERE client_id=$1 AND upload_batch_id=$2
            """,
            client_id,
            batch_id,
        )

    return {
        "status": "ok",
        "client_id": client_id,
        "upload_batch_id": str(batch_id),
        "total_rows": len(rows),
    }
