"""G'aznachilik to'lovlari API'sining namunaviy (mock) versiyasi.

Real integratsiyadagi kabi:
  * Bearer token bilan autentifikatsiya (token .env dan olinadi)
  * sahifalash (page / page_size) va o'sib boruvchi yuklash (updated_since)
  * ixtiyoriy "tartibsiz" rejim: tasodifiy 503 javoblar — mijozdagi retry
    mexanizmini sinash uchun (CHAOS_FAIL_RATE)

Ishga tushirish:  python -m pipeline api
"""
from __future__ import annotations

import hmac
import os
import random
from datetime import date
from functools import lru_cache

import polars as pl
from fastapi import Depends, FastAPI, Header, HTTPException, Query

from pipeline.config import load_settings

app = FastAPI(title="Treasury Payments API (mock)", version="1.0")
MAX_PAGE_SIZE = 100_000


@lru_cache(maxsize=1)
def _payments() -> pl.DataFrame:
    s = load_settings()
    return pl.read_parquet(s.paths.sources / "treasury" / "payments.parquet")


def require_token(authorization: str = Header(default="")) -> None:
    expected = load_settings().api_token
    scheme, _, token = authorization.partition(" ")
    # hmac.compare_digest — vaqt bo'yicha hujumlardan (timing attack) himoya
    if scheme.lower() != "bearer" or not hmac.compare_digest(token, expected):
        raise HTTPException(status_code=401, detail="Avtorizatsiya talab qilinadi")


def chaos() -> None:
    rate = float(os.getenv("CHAOS_FAIL_RATE", "0"))
    if rate and random.random() < rate:
        raise HTTPException(status_code=503, detail="Servis vaqtincha mavjud emas")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/v1/payments", dependencies=[Depends(require_token), Depends(chaos)])
def list_payments(
    updated_since: date | None = Query(default=None, description="Shu sanadan KEYINGI to'lovlar"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10_000, ge=1, le=MAX_PAGE_SIZE),
) -> dict:
    df = _payments()
    if updated_since is not None:
        df = df.filter(pl.col("payment_date") > updated_since)
    total = df.height
    pages = max(1, -(-total // page_size))
    chunk = df.slice((page - 1) * page_size, page_size)
    items = chunk.with_columns(pl.col("payment_date").cast(pl.Utf8)).to_dicts()
    return {"page": page, "page_size": page_size, "total": total, "pages": pages, "items": items}
