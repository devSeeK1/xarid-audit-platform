"""Transform qatlami: BRONZE → SILVER.

Vazifalar:
  * turli formatdagi summa va sanalarni yagona turga keltirish
  * takrorlarni olib tashlash
  * ma'lumot sifati qoidalari: buzilgan qatorlar KARANTINga sabab bilan yoziladi
  * shaxsiy ma'lumotlarni (PINFL) xeshlash — silver qatlamga ochiq PII o'tmaydi
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date

import polars as pl

from pipeline.config import Settings

log = logging.getLogger(__name__)

VALID_METHODS = ["tender", "auksion", "elektron_dokon", "togridan_togri"]
META_COLS = ["_source_file", "_ingested_at", "_run_id"]


# ----------------------------------------------------------------- parserlar
def parse_amount(col: str) -> pl.Expr:
    """'1250000.50' | '1 250 000,50' | '1,250,000.50' → 1250000.5"""
    s = pl.col(col).str.strip_chars().str.replace_all(r"[\s ]", "")
    us_thousands = s.str.contains(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$")
    decimal_comma = s.str.contains(",") & ~s.str.contains(r"\.")
    normalized = (
        pl.when(us_thousands).then(s.str.replace_all(",", ""))
        .when(decimal_comma).then(s.str.replace(",", "."))
        .otherwise(s)
    )
    return normalized.cast(pl.Float64, strict=False)


def parse_date(col: str) -> pl.Expr:
    """ISO (2024-03-15) va mahalliy (15.03.2024) formatlarini qo'llab-quvvatlaydi."""
    s = pl.col(col).str.strip_chars()
    return pl.coalesce(
        s.str.to_date("%Y-%m-%d", strict=False),
        s.str.to_date("%d.%m.%Y", strict=False),
    )


def normalize_method(col: str) -> pl.Expr:
    return pl.col(col).str.strip_chars().str.to_lowercase().str.replace_all(r"[\s\-]+", "_")


def valid_stir(col: str) -> pl.Expr:
    return pl.col(col).str.contains(r"^\d{9}$").fill_null(False)


def mask_pii(value: str | None, salt: str) -> str | None:
    """PINFL → qaytarib bo'lmaydigan kalit (SHA-256 + tuz). Bog'liqlikni topish mumkin, shaxsni emas."""
    if value is None or value == "":
        return None
    return hashlib.sha256(f"{salt}:{value.strip()}".encode()).hexdigest()[:16]


# ----------------------------------------------------------- sifat nazorati
@dataclass
class QualityResult:
    dataset: str
    rows_in: int = 0
    rows_out: int = 0
    checks: dict[str, int] = field(default_factory=dict)

    def rows(self) -> list[dict]:
        return [{"dataset": self.dataset, "check_name": k, "failed_rows": v} for k, v in self.checks.items()]


def apply_checks(df: pl.DataFrame, checks: list[tuple[str, pl.Expr]], qr: QualityResult
                 ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Har bir tekshiruv — (sabab, 'xato' sharti). Birinchi buzilgan qoida sabab sifatida yoziladi."""
    flags = df.with_columns([cond.fill_null(True).alias(f"__chk{i}") for i, (_, cond) in enumerate(checks)])
    for i, (name, _) in enumerate(checks):
        qr.checks[name] = qr.checks.get(name, 0) + int(flags[f"__chk{i}"].sum())
    reason = pl.lit(None, dtype=pl.Utf8)
    for i, (name, _) in reversed(list(enumerate(checks))):
        reason = pl.when(pl.col(f"__chk{i}")).then(pl.lit(name)).otherwise(reason)
    flags = flags.with_columns(reason.alias("reject_reason")).drop([f"__chk{i}" for i in range(len(checks))])
    good = flags.filter(pl.col("reject_reason").is_null()).drop("reject_reason")
    bad = flags.filter(pl.col("reject_reason").is_not_null())
    return good, bad


# ------------------------------------------------------------- datasetlar
def clean_buyers(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, QualityResult]:
    qr = QualityResult("buyers", rows_in=df.height)
    df = df.select(pl.col(["buyer_id", "buyer_name", "sector", "region", "budget_type"]).str.strip_chars())
    good, bad = apply_checks(df, [
        ("buyer_id bo'sh", pl.col("buyer_id").is_null()),
        ("takroriy buyer_id", pl.col("buyer_id").is_duplicated()),
    ], qr)
    qr.rows_out = good.height
    return good, bad, qr


def clean_suppliers(df: pl.DataFrame, salt: str) -> tuple[pl.DataFrame, pl.DataFrame, QualityResult]:
    qr = QualityResult("suppliers", rows_in=df.height)
    df = df.select(
        pl.col("STIR").str.strip_chars().alias("stir"),
        pl.col("Nomi").str.strip_chars().alias("name"),
        pl.col("Ro'yxatdan o'tgan sana").str.slice(0, 10).str.to_date(strict=False).alias("reg_date"),
        pl.col("Hudud").str.strip_chars().alias("region"),
        pl.col("Yuridik manzil").str.strip_chars().str.to_lowercase()
          .str.replace_all(r"\s+", " ").alias("address_norm"),
        pl.col("Direktor PINFL").map_elements(lambda v: mask_pii(v, salt), return_dtype=pl.Utf8)
          .alias("director_key"),
        pl.col("Holati").str.strip_chars().alias("status"),
        pl.col("Tugatilgan sana").str.slice(0, 10).str.to_date(strict=False).alias("liquidation_date"),
    )
    good, bad = apply_checks(df, [
        ("STIR formati noto'g'ri", ~valid_stir("stir")),
        ("takroriy STIR", pl.col("stir").is_duplicated()),
        ("ro'yxatdan o'tish sanasi yo'q", pl.col("reg_date").is_null()),
    ], qr)
    qr.rows_out = good.height
    return good, bad, qr


def clean_market_prices(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.col("toifa").str.strip_chars().alias("category"),
        pl.col("birlik").alias("unit"),
        (pl.col("oy") + "-01").str.to_date().alias("month"),
        pl.col("median_narx").cast(pl.Float64).alias("median_price"),
    )


def clean_contracts(df: pl.DataFrame, buyers: pl.DataFrame, suppliers: pl.DataFrame,
                    categories: list[str], period_start: date, period_end: date
                    ) -> tuple[pl.DataFrame, pl.DataFrame, QualityResult]:
    qr = QualityResult("contracts", rows_in=df.height)
    raw_cols = [c for c in df.columns if c not in META_COLS]
    before = df.height
    df = df.unique(subset=raw_cols, keep="first", maintain_order=True)
    qr.checks["aynan takroriy qator (olib tashlandi)"] = before - df.height

    df = df.select(
        pl.col("shartnoma_id").str.strip_chars().alias("contract_id"),
        pl.col("buyurtmachi_id").str.strip_chars().alias("buyer_id"),
        pl.col("yetkazuvchi_stir").str.strip_chars().alias("supplier_stir"),
        parse_date("sana").alias("contract_date"),
        normalize_method("xarid_usuli").alias("method"),
        pl.col("tovar_toifasi").str.strip_chars().alias("category"),
        pl.col("miqdor").cast(pl.Float64, strict=False).alias("quantity"),
        pl.col("birlik_narxi").cast(pl.Float64, strict=False).alias("unit_price"),
        parse_amount("summa").alias("amount"),
        pl.col("sana").alias("_raw_date"),
        pl.col("summa").alias("_raw_amount"),
        pl.col("_source_file"),
    )
    good, bad = apply_checks(df, [
        ("takroriy shartnoma_id (har xil mazmun)", pl.col("contract_id").is_duplicated()),
        ("yetkazuvchi STIR bo'sh", pl.col("supplier_stir").is_null()),
        ("STIR formati noto'g'ri", ~valid_stir("supplier_stir")),
        ("noma'lum buyurtmachi", ~pl.col("buyer_id").is_in(buyers["buyer_id"].implode())),
        ("yetkazuvchi reyestrda yo'q", ~pl.col("supplier_stir").is_in(suppliers["stir"].implode())),
        ("sana formati noto'g'ri", pl.col("contract_date").is_null()),
        ("sana davrdan tashqarida",
         ~pl.col("contract_date").is_between(pl.lit(period_start), pl.lit(period_end))),
        ("summa noto'g'ri yoki manfiy", pl.col("amount").is_null() | (pl.col("amount") <= 0)),
        ("noma'lum xarid usuli", ~pl.col("method").is_in(VALID_METHODS)),
        ("noma'lum tovar toifasi", ~pl.col("category").is_in(categories)),
        ("summa miqdor×narxga mos emas",
         ((pl.col("quantity") * pl.col("unit_price") - pl.col("amount")).abs() / pl.col("amount")) > 0.01),
    ], qr)
    good = good.drop(["_raw_date", "_raw_amount", "_source_file"])
    qr.rows_out = good.height
    return good, bad, qr


def clean_payments(lf: pl.LazyFrame, contracts: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, QualityResult]:
    df = lf.collect()
    qr = QualityResult("payments", rows_in=df.height)
    before = df.height
    df = df.unique(subset=["payment_id"], keep="first", maintain_order=True)
    qr.checks["takroriy payment_id (API qayta yuborgan)"] = before - df.height
    df = df.select(
        pl.col("payment_id").str.strip_chars(),
        pl.col("contract_id").str.strip_chars(),
        pl.col("payment_date").str.to_date(strict=False),
        pl.col("amount").cast(pl.Float64),
        pl.col("treasury_account"),
    )
    good, bad = apply_checks(df, [
        ("shartnoma_id bo'sh", pl.col("contract_id").is_null()),
        ("noma'lum yoki karantindagi shartnoma",
         ~pl.col("contract_id").is_in(contracts["contract_id"].implode())),
        ("to'lov sanasi noto'g'ri", pl.col("payment_date").is_null()),
        ("to'lov summasi noto'g'ri", pl.col("amount").is_null() | (pl.col("amount") <= 0)),
    ], qr)
    qr.rows_out = good.height
    return good, bad, qr


def run_transform(settings: Settings) -> list[QualityResult]:
    p = settings.paths
    period_start = date.fromisoformat(settings.period["start"])
    period_end = date.fromisoformat(settings.period["end"])
    results: list[QualityResult] = []

    def save(name: str, good: pl.DataFrame, bad: pl.DataFrame | None, qr: QualityResult | None) -> None:
        good.write_parquet(p.silver / f"{name}.parquet")
        if bad is not None:
            bad.write_parquet(p.quarantine / f"{name}.parquet")
        if qr is not None:
            results.append(qr)
            q = bad.height if bad is not None else 0
            log.info("SILVER %-12s kirdi=%9d  o'tdi=%9d  karantin=%7d", name, qr.rows_in, qr.rows_out, q)

    buyers, bad_b, qr_b = clean_buyers(pl.read_parquet(p.bronze / "buyers.parquet"))
    save("buyers", buyers, bad_b, qr_b)
    suppliers, bad_s, qr_s = clean_suppliers(pl.read_parquet(p.bronze / "suppliers.parquet"), settings.pii_salt)
    save("suppliers", suppliers, bad_s, qr_s)
    prices = clean_market_prices(pl.read_parquet(p.bronze / "market_prices.parquet"))
    save("market_prices", prices, None, None)
    categories = prices["category"].unique().to_list()
    contracts, bad_c, qr_c = clean_contracts(pl.read_parquet(p.bronze / "contracts.parquet"),
                                             buyers, suppliers, categories, period_start, period_end)
    save("contracts", contracts, bad_c, qr_c)
    pay_lf = pl.scan_parquet(str(p.bronze / "treasury_payments" / "run=*" / "*.parquet"))
    payments, bad_p, qr_p = clean_payments(pay_lf, contracts)
    save("payments", payments, bad_p, qr_p)
    return results
