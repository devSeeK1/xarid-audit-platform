"""Audit qoidalari dvigateli.

Har bir qoida — alohida, o'qilishi oson SQL fayl (sql/rules/). Parametrlar
config/settings.yaml dan olinadi va SQL'ga qo'yilishidan oldin RAQAM ekanligi
tekshiriladi (SQL in'eksiyadan himoya).
"""
from __future__ import annotations

import logging
import numbers
import re
import time
from dataclasses import dataclass

import duckdb

from pipeline.config import ROOT, Settings

log = logging.getLogger(__name__)
RULES_DIR = ROOT / "sql" / "rules"


@dataclass(frozen=True)
class Rule:
    code: str
    file: str
    name: str
    description: str
    severity: str


RULES = [
    Rule("R01", "R01_split_purchase.sql", "Xaridni bo'lib-bo'lib amalga oshirish",
         "Tender chegarasidan qochish uchun bitta xarid bir nechta kichik to'g'ridan-to'g'ri shartnomaga bo'lingan.",
         "Yuqori"),
    Rule("R02", "R02_overpayment.sql", "Ortiqcha to'lov",
         "Shartnoma bo'yicha to'langan summa shartnoma qiymatidan oshib ketgan.", "Yuqori"),
    Rule("R03", "R03_price_inflation.sql", "Narxning sun'iy oshirilishi",
         "Birlik narxi shu oydagi bozor medianasidan belgilangan koeffitsiyentdan ko'proq yuqori.", "O'rta"),
    Rule("R04", "R04_new_company.sql", "Yangi tashkil etilgan korxona",
         "Ro'yxatdan o'tganiga oz vaqt bo'lgan korxona bilan yirik shartnoma tuzilgan.", "O'rta"),
    Rule("R05", "R05_affiliated_suppliers.sql", "Affillangan yetkazuvchilar",
         "Direktori yoki manzili bir xil korxonalar bitta buyurtmachida g'olib bo'lgan.", "Yuqori"),
    Rule("R06", "R06_supplier_concentration.sql", "Yetkazuvchi konsentratsiyasi",
         "Bitta yetkazuvchi buyurtmachi yillik xarajatining katta qismini olgan.", "Past"),
    Rule("R07", "R07_payment_before_contract.sql", "Shartnomadan oldin to'lov",
         "To'lov shartnoma tuzilgan sanadan oldin amalga oshirilgan.", "Yuqori"),
    Rule("R08", "R08_payment_to_liquidated.sql", "Tugatilgan korxonaga to'lov",
         "To'lov faoliyati rasman tugatilgan korxonaga o'tkazilgan.", "Juda yuqori"),
]

FINDINGS_DDL = """
CREATE TABLE IF NOT EXISTS audit.findings (
    run_id VARCHAR, rule_code VARCHAR, entity_type VARCHAR, entity_id VARCHAR,
    buyer_id VARCHAR, supplier_stir VARCHAR, amount_at_risk DOUBLE, details VARCHAR,
    detected_at TIMESTAMP DEFAULT current_timestamp
);
"""


def render(sql: str, params: dict) -> str:
    def repl(m: re.Match) -> str:
        key = m.group(1)
        if key not in params:
            raise KeyError(f"SQL parametri topilmadi: {key}")
        val = params[key]
        if isinstance(val, bool) or not isinstance(val, numbers.Number):
            raise ValueError(f"Parametr {key} raqam bo'lishi kerak, berildi: {val!r}")
        return repr(val)
    return re.sub(r"\{([a-z_]+)\}", repl, sql)


def run_rules(con: duckdb.DuckDBPyConnection, settings: Settings, run_id: str) -> dict[str, int]:
    con.execute(FINDINGS_DDL)
    con.execute("DELETE FROM audit.findings WHERE run_id = ?", [run_id])
    con.execute("""CREATE OR REPLACE TABLE audit.rule_catalog (
                     rule_code VARCHAR, name VARCHAR, description VARCHAR, severity VARCHAR, weight INTEGER)""")
    con.executemany("INSERT INTO audit.rule_catalog VALUES (?, ?, ?, ?, ?)",
                    [[r.code, r.name, r.description, r.severity, settings.rule_weights[r.code]] for r in RULES])
    counts = {}
    for rule in RULES:
        t0 = time.perf_counter()
        sql = render((RULES_DIR / rule.file).read_text(encoding="utf-8"), settings.thresholds)
        con.execute(f"""INSERT INTO audit.findings
                        (run_id, rule_code, entity_type, entity_id, buyer_id, supplier_stir, amount_at_risk, details)
                        SELECT ?, * FROM ({sql})""", [run_id])
        n = con.execute("SELECT COUNT(*) FROM audit.findings WHERE run_id = ? AND rule_code = ?",
                        [run_id, rule.code]).fetchone()[0]
        counts[rule.code] = n
        log.info("AUDIT %s %-40s %6d topildi (%.2f s)", rule.code, rule.name, n, time.perf_counter() - t0)

    con.execute("""
        CREATE OR REPLACE TABLE audit.buyer_risk AS
        SELECT f.buyer_id, b.buyer_name, b.region, b.sector,
               COUNT(*) AS findings,
               SUM(rc.weight) AS risk_score,
               SUM(f.amount_at_risk) AS amount_at_risk,
               list_sort(list(DISTINCT f.rule_code)) AS rules
        FROM audit.findings f
        JOIN audit.rule_catalog rc USING (rule_code)
        JOIN dwh.dim_buyer b USING (buyer_id)
        WHERE f.run_id = ?
        GROUP BY ALL
        ORDER BY risk_score DESC, amount_at_risk DESC""", [run_id])
    return counts
