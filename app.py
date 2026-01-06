from __future__ import annotations
#ssr-api/app.py
from fastapi import FastAPI
from .db import init_db_pool, close_db_pool
from .ssr import router as ssr_router

app = FastAPI(title="SSR Equity API")

app.include_router(ssr_router)


@app.on_event("startup")
async def _startup():
    await init_db_pool()


@app.on_event("shutdown")
async def _shutdown():
    await close_db_pool()
