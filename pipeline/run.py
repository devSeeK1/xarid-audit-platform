"""Orkestrator: Extract → Transform → Load → Audit → (Spark) → Hisobot.

Muhim tamoyillar:
  * har bir ishga tushirish run_id bilan jurnalga yoziladi (kim, qachon, qayerdan)
  * watermark faqat BUTUN jarayon muvaffaqiyatli tugagandan keyin yangilanadi —
    xato bo'lsa, keyingi ishga tushirish ma'lumotni yo'qotmasdan qaytadan oladi
  * har bir bosqich vaqti o'lchanadi (meta.step_timings)
"""
from __future__ import annotations

import getpass
import logging
import os
import platform
import secrets
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime

import httpx

from pipeline.audit.engine import run_rules
from pipeline.config import ROOT, Settings
from pipeline.extract import sources
from pipeline.load import warehouse as wh
from pipeline.transform.clean import run_transform

log = logging.getLogger(__name__)
WATERMARK_SOURCE = "treasury_payments"


def _api_alive(settings: Settings) -> bool:
    try:
        return httpx.get(f"{settings.api_base_url}/health", timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


@contextmanager
def treasury_api(settings: Settings, autostart: bool):
    """API ishlamayotgan bo'lsa va autostart=True — mock API'ni fon jarayonida ishga tushiradi."""
    proc = None
    if not _api_alive(settings):
        if not autostart:
            raise RuntimeError(f"G'aznachilik API'si javob bermayapti: {settings.api_base_url}. "
                               "Avval `python -m pipeline api` ni ishga tushiring yoki --start-api bering.")
        api = settings.api
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "mock_api.app:app", "--host", api["host"],
             "--port", str(api["port"]), "--log-level", "warning"],
            cwd=ROOT, env=os.environ.copy())
        for _ in range(60):
            if _api_alive(settings):
                break
            time.sleep(0.5)
        else:
            proc.terminate()
            raise RuntimeError("Mock API ishga tushmadi")
        log.info("Mock API ishga tushirildi (pid=%s)", proc.pid)
    try:
        yield
    finally:
        if proc is not None:
            proc.terminate()
            proc.wait(timeout=10)


class Timer:
    def __init__(self, con, run_id: str):
        self.con, self.run_id, self.items = con, run_id, {}

    @contextmanager
    def step(self, name: str):
        t0 = time.perf_counter()
        log.info("── %s", name)
        yield
        sec = time.perf_counter() - t0
        self.items[name] = sec
        wh.save_timing(self.con, self.run_id, name, sec)


def reconcile_spark(con, settings: Settings, run_id: str) -> dict[str, int]:
    """Spark natijalarini DuckDB bilan solishtirish."""
    marts = str(settings.paths.marts).replace("\\", "/")
    diff_sy = con.execute(f"""
        WITH d AS (
            SELECT s.stir AS supplier_stir, year(c.contract_date) AS year,
                   COUNT(*) AS contracts, SUM(c.amount) AS contract_amount,
                   SUM(COALESCE(p.paid, 0)) AS paid_amount
            FROM dwh.fact_contract c JOIN dwh.dim_supplier s USING (supplier_key)
            LEFT JOIN (SELECT contract_id, SUM(amount) paid FROM dwh.fact_payment GROUP BY 1) p USING (contract_id)
            GROUP BY ALL),
        sp AS (SELECT * FROM read_parquet('{marts}/spark_supplier_year/*.parquet'))
        SELECT COUNT(*) FROM d FULL JOIN sp USING (supplier_stir, year)
        WHERE d.contracts IS DISTINCT FROM sp.contracts
           OR abs(coalesce(d.contract_amount, -1) - coalesce(sp.contract_amount, -2)) > 0.01
           OR abs(coalesce(d.paid_amount, -1) - coalesce(sp.paid_amount, -2)) > 0.01
    """).fetchone()[0]
    diff_r02 = con.execute(f"""
        WITH a AS (SELECT entity_id AS contract_id FROM audit.findings WHERE run_id = ? AND rule_code = 'R02'),
             b AS (SELECT contract_id FROM read_parquet('{marts}/spark_overpaid_contracts/*.parquet'))
        SELECT COUNT(*) FROM a FULL JOIN b USING (contract_id) WHERE a.contract_id IS NULL OR b.contract_id IS NULL
    """, [run_id]).fetchone()[0]
    res = {"spark_vs_duckdb: yetkazuvchi×yil farqlari": diff_sy,
           "spark_vs_duckdb: R02 ortiqcha to'lov farqlari": diff_r02}
    con.executemany("INSERT INTO meta.dq_results VALUES (?, 'spark_reconciliation', ?, ?)",
                    [[run_id, k, v] for k, v in res.items()])
    log.info("RECONCILIATION Spark ↔ DuckDB: %s", res)
    return res


def run_pipeline(settings: Settings, full_refresh: bool = False, start_api: bool = False,
                 with_spark: bool = False, build_report: bool = True) -> str:
    settings.paths.ensure()
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + secrets.token_hex(3)
    con = wh.connect(settings)
    wh.init_meta(con)
    if full_refresh:
        wh.reset_watermark(con, WATERMARK_SOURCE)
    since = wh.get_watermark(con, WATERMARK_SOURCE)
    mode = "incremental" if since else "full"
    wh.run_started(con, run_id, getpass.getuser(), platform.node(), mode)
    log.info("════ RUN %s  (rejim: %s, watermark: %s)", run_id, mode, since)
    timer = Timer(con, run_id)
    t_start = time.perf_counter()
    try:
        with timer.step("1. Extract (manbalar → bronze)"):
            lineage = [
                sources.extract_csv(settings, run_id, "contracts", "xarid_portal/contracts_2024_2025.csv", ";"),
                sources.extract_excel(settings, run_id, "suppliers", "soliq_registry/suppliers.xlsx", "Reyestr"),
                sources.extract_csv(settings, run_id, "buyers", "reference/buyers.csv"),
                sources.extract_csv(settings, run_id, "market_prices", "statistika/market_prices.csv"),
            ]
            with treasury_api(settings, start_api):
                api_lineage, new_watermark = sources.extract_payments_api(
                    settings, run_id, since, full_refresh=full_refresh or since is None)
            lineage.append(api_lineage)
            wh.save_lineage(con, run_id, [l.as_dict() for l in lineage])

        with timer.step("2. Transform + sifat nazorati (bronze → silver)"):
            quality = run_transform(settings)
            wh.save_quality(con, run_id, quality)

        with timer.step("3. Load (silver → DWH)"):
            wh.build_warehouse(con, settings)

        with timer.step("4. Audit qoidalari"):
            run_rules(con, settings, run_id)

        if with_spark:
            with timer.step("5. Spark job + reconciliation"):
                from spark_jobs.supplier_spend import run as spark_run
                stats = spark_run(settings.paths.silver, settings.paths.marts,
                                  float(settings.thresholds["overpayment_tolerance"]))
                log.info("SPARK %s", stats)
                reconcile_spark(con, settings, run_id)

        wh.set_watermark(con, WATERMARK_SOURCE, new_watermark)
        total = time.perf_counter() - t_start
        wh.run_finished(con, run_id, "success", total)
        log.info("════ RUN %s muvaffaqiyatli tugadi: %.1f s", run_id, total)
    except Exception as exc:
        wh.run_finished(con, run_id, "failed", time.perf_counter() - t_start, repr(exc)[:500])
        log.exception("RUN %s xato bilan tugadi", run_id)
        raise
    finally:
        con.close()

    if build_report:
        from pipeline.report.build import build_report as _build
        _build(settings, run_id)
    return run_id
