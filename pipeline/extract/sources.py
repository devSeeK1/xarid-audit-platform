"""Extract qatlami: manba tizimlaridan ma'lumotni o'zgartirmasdan BRONZE qatlamiga olish.

Har bir manba uchun "lineage" (kelib chiqish) yozuvi saqlanadi: fayl yo'li,
SHA-256 nazorat yig'indisi, qatorlar soni. Bu keyinchalik ma'lumot
o'zgartirilmaganini isbotlash imkonini beradi (yaxlitlik nazorati).
"""
from __future__ import annotations

import hashlib
import logging
import random
import shutil
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import polars as pl

from pipeline.config import Settings

log = logging.getLogger(__name__)


@dataclass
class Lineage:
    source: str
    location: str
    sha256: str
    rows: int
    extracted_at: str

    def as_dict(self) -> dict:
        return asdict(self)


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _with_meta(df: pl.DataFrame, source_file: str, run_id: str) -> pl.DataFrame:
    return df.with_columns(
        pl.lit(source_file).alias("_source_file"),
        pl.lit(_now()).alias("_ingested_at"),
        pl.lit(run_id).alias("_run_id"),
    )


def extract_csv(settings: Settings, run_id: str, name: str, rel_path: str, separator: str = ",") -> Lineage:
    path = settings.paths.sources / rel_path
    # Barcha ustunlarni MATN sifatida o'qiymiz: bronze — "xom" nusxa, turlarni silver aniqlaydi
    df = pl.read_csv(path, separator=separator, infer_schema=False, encoding="utf8")
    df = df.rename({c: c.lstrip("﻿").strip() for c in df.columns})
    out = settings.paths.bronze / f"{name}.parquet"
    _with_meta(df, rel_path, run_id).write_parquet(out)
    lin = Lineage(name, rel_path, sha256_file(path), df.height, _now())
    log.info("CSV  %-22s %9d qator  sha256=%s…", name, df.height, lin.sha256[:12])
    return lin


def extract_excel(settings: Settings, run_id: str, name: str, rel_path: str, sheet: str) -> Lineage:
    path = settings.paths.sources / rel_path
    df = pl.read_excel(path, sheet_name=sheet)
    df = df.with_columns(pl.all().cast(pl.Utf8))
    out = settings.paths.bronze / f"{name}.parquet"
    _with_meta(df, rel_path, run_id).write_parquet(out)
    lin = Lineage(name, rel_path, sha256_file(path), df.height, _now())
    log.info("XLSX %-22s %9d qator  sha256=%s…", name, df.height, lin.sha256[:12])
    return lin


PAYMENT_SCHEMA = {"payment_id": pl.Utf8, "contract_id": pl.Utf8, "payment_date": pl.Utf8,
                  "amount": pl.Float64, "treasury_account": pl.Utf8}


def _get_with_retry(client: httpx.Client, url: str, params: dict, max_retries: int) -> httpx.Response:
    """Eksponensial kutish + jitter bilan qayta urinish (429, 5xx, tarmoq xatolari)."""
    for attempt in range(max_retries + 1):
        try:
            resp = client.get(url, params=params)
            if resp.status_code == 401:
                raise PermissionError("API token noto'g'ri yoki yo'q (401)")
            if resp.status_code in (429, 500, 502, 503, 504):
                raise httpx.HTTPStatusError(f"HTTP {resp.status_code}", request=resp.request, response=resp)
            resp.raise_for_status()
            return resp
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            if attempt == max_retries:
                raise
            wait = min(30.0, 0.5 * 2 ** attempt) * (0.5 + random.random())
            log.warning("API xatosi (%s), %.1f s dan keyin qayta urinish %d/%d",
                        exc, wait, attempt + 1, max_retries)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def extract_payments_api(settings: Settings, run_id: str, since: date | None,
                         full_refresh: bool = False) -> tuple[Lineage, date | None]:
    """G'aznachilik API'sidan to'lovlarni sahifalab, o'sib boruvchi (incremental) tarzda olish."""
    api = settings.api
    base = settings.paths.bronze / "treasury_payments"
    if full_refresh and base.exists():
        shutil.rmtree(base)
    out_dir = base / f"run={run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    headers = {"Authorization": f"Bearer {settings.api_token}", "Accept": "application/json"}
    params = {"page_size": api["page_size"]}
    if since is not None:
        params["updated_since"] = since.isoformat()

    h = hashlib.sha256()
    total_rows, page, pages, max_date = 0, 1, None, since
    with httpx.Client(base_url=settings.api_base_url, headers=headers, timeout=api["timeout_seconds"]) as client:
        while pages is None or page <= pages:
            resp = _get_with_retry(client, "/api/v1/payments", {**params, "page": page}, api["max_retries"])
            h.update(resp.content)
            body = resp.json()
            pages = body["pages"]
            if body["items"]:
                df = pl.DataFrame(body["items"], schema=PAYMENT_SCHEMA)
                _with_meta(df, "treasury_api:/api/v1/payments", run_id).write_parquet(
                    out_dir / f"part-{page:05d}.parquet")
                total_rows += df.height
                page_max = df["payment_date"].max()
                if page_max:
                    d = date.fromisoformat(page_max)
                    max_date = d if max_date is None or d > max_date else max_date
            log.info("API  sahifa %d/%d  (%d qator)", page, pages, len(body["items"]))
            page += 1
    if total_rows == 0:
        shutil.rmtree(out_dir, ignore_errors=True)
    lin = Lineage("treasury_payments", f"{settings.api_base_url}/api/v1/payments"
                  + (f"?updated_since={since}" if since else ""), h.hexdigest(), total_rows, _now())
    log.info("API  %-22s %9d yangi qator (watermark: %s → %s)", "treasury_payments", total_rows, since, max_date)
    return lin, max_date
