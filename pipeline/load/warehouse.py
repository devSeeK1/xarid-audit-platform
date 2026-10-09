"""Load qatlami: SILVER → DuckDB ma'lumotlar ombori (GOLD) + meta jadvallar."""
from __future__ import annotations

import logging
from datetime import date, datetime

import duckdb

from pipeline.config import ROOT, Settings

log = logging.getLogger(__name__)
SQL_DIR = ROOT / "sql"


def connect(settings: Settings, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    settings.paths.warehouse.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(settings.paths.warehouse), read_only=read_only)


def init_meta(con: duckdb.DuckDBPyConnection) -> None:
    con.execute((SQL_DIR / "meta.sql").read_text(encoding="utf-8"))


def _sql_path(p) -> str:
    return str(p).replace("\\", "/").replace("'", "''")


def build_warehouse(con: duckdb.DuckDBPyConnection, settings: Settings) -> dict[str, int]:
    sql = (SQL_DIR / "warehouse.sql").read_text(encoding="utf-8").replace("{silver}", _sql_path(settings.paths.silver))
    con.execute(sql)
    counts = {}
    for t in ("dim_buyer", "dim_supplier", "dim_category", "fact_contract", "fact_payment"):
        counts[t] = con.execute(f"SELECT COUNT(*) FROM dwh.{t}").fetchone()[0]
    log.info("DWH  yuklandi: %s", counts)
    return counts


# ------------------------------------------------------------------ meta
def get_watermark(con, source: str) -> date | None:
    row = con.execute("SELECT last_value FROM meta.watermarks WHERE source = ?", [source]).fetchone()
    return row[0] if row else None


def set_watermark(con, source: str, value: date | None) -> None:
    if value is None:
        return
    con.execute("""INSERT INTO meta.watermarks VALUES (?, ?, ?)
                   ON CONFLICT (source) DO UPDATE SET last_value = excluded.last_value,
                                                      updated_at = excluded.updated_at""",
                [source, value, datetime.now()])


def reset_watermark(con, source: str) -> None:
    con.execute("DELETE FROM meta.watermarks WHERE source = ?", [source])


def run_started(con, run_id: str, user: str, host: str, mode: str) -> None:
    con.execute("INSERT INTO meta.etl_runs (run_id, started_at, status, run_by, host, mode) VALUES (?, ?, 'running', ?, ?, ?)",
                [run_id, datetime.now(), user, host, mode])


def run_finished(con, run_id: str, status: str, duration: float, error: str | None = None) -> None:
    con.execute("UPDATE meta.etl_runs SET finished_at = ?, status = ?, duration_sec = ?, error = ? WHERE run_id = ?",
                [datetime.now(), status, duration, error, run_id])


def save_lineage(con, run_id: str, records: list[dict]) -> None:
    con.executemany("INSERT INTO meta.source_lineage VALUES (?, ?, ?, ?, ?, ?)",
                    [[run_id, r["source"], r["location"], r["sha256"], r["rows"], r["extracted_at"]] for r in records])


def save_quality(con, run_id: str, results) -> None:
    for qr in results:
        con.executemany("INSERT INTO meta.dq_results VALUES (?, ?, ?, ?)",
                        [[run_id, r["dataset"], r["check_name"], r["failed_rows"]] for r in qr.rows()])
        quarantined = qr.rows_in - qr.rows_out - sum(
            v for k, v in qr.checks.items() if "olib tashlandi" in k or "takroriy payment_id" in k)
        con.execute("INSERT INTO meta.dataset_stats VALUES (?, ?, ?, ?, ?)",
                    [run_id, qr.dataset, qr.rows_in, qr.rows_out, quarantined])


def save_timing(con, run_id: str, step: str, seconds: float) -> None:
    con.execute("INSERT INTO meta.step_timings VALUES (?, ?, ?)", [run_id, step, round(seconds, 2)])
