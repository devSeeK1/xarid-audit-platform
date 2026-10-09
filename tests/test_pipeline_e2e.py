"""To'liq jarayon testi: har bir qoida ataylab qo'shilgan BARCHA anomaliyalarni topishi kerak."""
import json

import duckdb
import polars as pl
import pytest


@pytest.fixture(scope="module")
def ctx(built, settings):
    truth = json.loads((settings.paths.sources / "_ground_truth.json").read_text(encoding="utf-8"))
    con = duckdb.connect(str(settings.paths.warehouse), read_only=True)
    yield truth, con, built
    con.close()


def findings(con, run_id, code):
    return con.execute("SELECT entity_id, supplier_stir FROM audit.findings WHERE run_id = ? AND rule_code = ?",
                       [run_id, code]).fetchall()


def test_run_logged_successfully(ctx):
    _, con, run_id = ctx
    assert con.execute("SELECT status FROM meta.etl_runs WHERE run_id = ?", [run_id]).fetchone()[0] == "success"


def test_r01_split_purchase_recall(ctx):
    truth, con, run_id = ctx
    got = {tuple(e.split("|")) for e, _ in findings(con, run_id, "R01")}
    assert {(b, str(s), c) for b, s, c in truth["R01_split_groups"]} <= got


@pytest.mark.parametrize("code, key", [("R02", "R02_overpaid_contracts"), ("R03", "R03_price_contracts"),
                                       ("R07", "R07_early_payments")])
def test_entity_rules_recall_and_precision(ctx, code, key):
    truth, con, run_id = ctx
    assert {e for e, _ in findings(con, run_id, code)} == set(truth[key])


@pytest.mark.parametrize("code, key", [("R04", "R04_new_company_stirs"), ("R08", "R08_liquidated_stirs")])
def test_supplier_rules(ctx, code, key):
    truth, con, run_id = ctx
    assert {s for _, s in findings(con, run_id, code)} == {str(x) for x in truth[key]}


def test_r05_affiliated_clusters(ctx):
    truth, con, run_id = ctx
    got = {e for e, _ in findings(con, run_id, "R05")}
    for c in truth["R05_affiliated_clusters"]:
        assert f"{c['buyer_id']}|{','.join(sorted(map(str, c['stirs'])))}" in got


def test_r06_concentration(ctx):
    truth, con, run_id = ctx
    got = {e for e, _ in findings(con, run_id, "R06")}
    assert {f"{b}|{y}|{s}" for b, y, s in truth["R06_concentration"]} <= got


def test_dirty_rows_are_quarantined_not_lost(ctx, settings):
    _, con, run_id = ctx
    q = pl.read_parquet(settings.paths.quarantine / "contracts.parquet")
    assert q.height > 0 and q["reject_reason"].null_count() == 0
    reasons = set(q["reject_reason"].unique())
    assert {"STIR formati noto'g'ri", "noma'lum buyurtmachi", "summa noto'g'ri yoki manfiy"} & reasons


def test_no_raw_pii_in_silver_or_warehouse(ctx, settings):
    _, con, _ = ctx
    cols = pl.read_parquet(settings.paths.silver / "suppliers.parquet").columns
    assert "director_key" in cols and not any("pinfl" in c.lower() for c in cols)
    wh_cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns").fetchall()]
    assert not any("pinfl" in c.lower() for c in wh_cols)


def test_lineage_has_checksums(ctx):
    _, con, run_id = ctx
    rows = con.execute("SELECT source, sha256 FROM meta.source_lineage WHERE run_id = ?", [run_id]).fetchall()
    assert len(rows) == 5 and all(len(sha) == 64 for _, sha in rows)


def test_report_generated(built, settings):
    html = (settings.paths.reports / "audit_report.html").read_text(encoding="utf-8")
    assert "Xarid Audit Hisoboti" in html and "R08" in html
