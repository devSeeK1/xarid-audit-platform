"""Bitta shartnoma yoki yetkazuvchi bo'yicha "ish yig'majildi" (auditor uchun).

    python -m pipeline tekshir C0438999      # shartnoma
    python -m pipeline tekshir 915392519     # yetkazuvchi (STIR)
    python -m pipeline tekshir P01437186     # to'lov (tegishli shartnoma ochiladi)

Hamma ma'lumot omborning o'zidan olinadi: shartnoma tafsilotlari, bozor narxi,
to'lovlar, yetkazuvchi va unga bog'liq korxonalar, hamda qaysi qoidalar signal bergani.
"""
from __future__ import annotations

import duckdb

from pipeline.config import Settings
from pipeline.load.warehouse import connect
from pipeline.report.build import fmt_int, fmt_som, humanize_details


def _latest_run(con) -> str:
    return con.execute("SELECT run_id FROM meta.etl_runs WHERE status = 'success' "
                       "ORDER BY started_at DESC LIMIT 1").fetchone()[0]


def _title(text: str) -> None:
    print(f"\n{text}\n{'─' * len(text)}")


def _row(label: str, value) -> None:
    print(f"  {label:<26} {value}")


def _print_findings(rows) -> None:
    if not rows:
        print("  Hech qanday qoida signal bermadi.")
        return
    for code, name, severity, entity, amount, details in rows:
        print(f"  [{code}] {name}  ·  daraja: {severity}  ·  xavf ostida: {fmt_som(amount)}")
        print(f"        obyekt: {entity}")
        print("        " + "; ".join(f"{k}: {v}" for k, v in humanize_details(details)))


def inspect_contract(con: duckdb.DuckDBPyConnection, contract_id: str) -> bool:
    c = con.execute("""SELECT contract_id, contract_date, method, category, quantity, unit_price, amount,
                              buyer_id, buyer_name, buyer_region, sector,
                              supplier_stir, supplier_name, supplier_reg_date, supplier_status, liquidation_date
                       FROM dwh.v_contracts WHERE contract_id = ?""", [contract_id]).fetchone()
    if not c:
        return False
    run = _latest_run(con)
    (cid, cdate, method, cat, qty, price, amount, bid, bname, bregion, sector,
     stir, sname, sreg, sstatus, sliq) = c

    _title(f"SHARTNOMA {cid}")
    _row("Sana", cdate)
    _row("Xarid usuli", method)
    _row("Toifa", cat)
    _row("Miqdor × narx", f"{fmt_int(qty)} × {fmt_som(price)}")
    _row("Summa", fmt_som(amount))

    m = con.execute("""SELECT median_price FROM dwh.ref_market_price
                       WHERE category = ? AND month = date_trunc('month', ?::DATE)::DATE""", [cat, cdate]).fetchone()
    if m:
        ratio = price / m[0]
        flag = "  ← bozordan sezilarli qimmat" if ratio > 1.8 else ""
        _row("Bozor medianasi (shu oy)", f"{fmt_som(m[0])}  (nisbat {ratio:.2f}){flag}")

    _title("BUYURTMACHI")
    _row("ID / nomi", f"{bid} · {bname}")
    _row("Hudud · soha", f"{bregion} · {sector}")

    _title("YETKAZUVCHI")
    _row("STIR / nomi", f"{stir} · {sname}")
    _row("Ro'yxatdan o'tgan", f"{sreg}  ({(cdate - sreg).days} kun shartnomagacha)")
    _row("Holati", sstatus + (f" (tugatilgan: {sliq})" if sliq else ""))

    pays = con.execute("""SELECT payment_id, payment_date, amount FROM dwh.fact_payment
                          WHERE contract_id = ? ORDER BY payment_date""", [cid]).fetchall()
    _title(f"TO'LOVLAR ({len(pays)} ta)")
    total = 0.0
    for pid, pdate, pamt in pays:
        total += pamt
        notes = []
        if pdate < cdate:
            notes.append("shartnomadan OLDIN")
        if sliq and pdate > sliq:
            notes.append("korxona tugatilgandan KEYIN")
        print(f"  {pid}  {pdate}  {fmt_som(pamt):>22}" + (f"   ← {', '.join(notes)}" if notes else ""))
    pct = 100 * total / amount if amount else 0
    _row("Jami to'langan", f"{fmt_som(total)}  ({pct:.1f}% shartnomadan)")

    _title("AUDIT SIGNALLARI")
    rows = con.execute("""
        SELECT f.rule_code, rc.name, rc.severity, f.entity_id, f.amount_at_risk, f.details
        FROM audit.findings f JOIN audit.rule_catalog rc USING (rule_code)
        WHERE f.run_id = ? AND (
              f.entity_id = ?
           OR f.entity_id IN (SELECT payment_id FROM dwh.fact_payment WHERE contract_id = ?)
           OR (f.rule_code = 'R01' AND f.entity_id = ? || '|' || ? || '|' || ?)
           OR (f.rule_code IN ('R05', 'R06') AND f.buyer_id = ? AND f.entity_id LIKE '%' || ? || '%'))
        ORDER BY f.rule_code""", [run, cid, cid, bid, stir, cat, bid, stir]).fetchall()
    _print_findings(rows)
    return True


def inspect_supplier(con: duckdb.DuckDBPyConnection, stir: str) -> bool:
    s = con.execute("""SELECT stir, name, reg_date, region, status, liquidation_date, director_key, address_norm
                       FROM dwh.dim_supplier WHERE stir = ?""", [stir]).fetchone()
    if not s:
        return False
    run = _latest_run(con)
    _, name, reg, region, status, liq, dkey, addr = s
    _title(f"YETKAZUVCHI {stir} · {name}")
    _row("Ro'yxatdan o'tgan", reg)
    _row("Hudud", region)
    _row("Holati", status + (f" (tugatilgan: {liq})" if liq else ""))
    _row("Direktor kaliti (xesh)", dkey)

    n, total, buyers = con.execute("""SELECT COUNT(*), COALESCE(SUM(amount), 0), COUNT(DISTINCT buyer_id)
                                      FROM dwh.v_contracts WHERE supplier_stir = ?""", [stir]).fetchone()
    _row("Shartnomalar", f"{fmt_int(n)} ta, {fmt_som(total)}, {buyers} ta buyurtmachi")

    related = con.execute("""SELECT stir, name,
                                    CASE WHEN director_key = ? THEN 'direktor bir xil' ELSE 'manzil bir xil' END
                             FROM dwh.dim_supplier
                             WHERE stir <> ? AND (director_key = ? OR address_norm = ?)""",
                          [dkey, stir, dkey, addr]).fetchall()
    _title(f"BOG'LIQ KORXONALAR ({len(related)} ta)")
    for rs, rn, why in related:
        print(f"  {rs} · {rn}  ({why})")
    if not related:
        print("  Topilmadi.")

    top = con.execute("""SELECT buyer_id, COUNT(*), SUM(amount) FROM dwh.v_contracts WHERE supplier_stir = ?
                         GROUP BY 1 ORDER BY 3 DESC LIMIT 5""", [stir]).fetchall()
    _title("ASOSIY BUYURTMACHILAR")
    for b, cnt, amt in top:
        print(f"  {b}  {cnt:>4} ta  {fmt_som(amt):>22}")

    _title("AUDIT SIGNALLARI")
    rows = con.execute("""SELECT f.rule_code, rc.name, rc.severity, f.entity_id, f.amount_at_risk, f.details
                          FROM audit.findings f JOIN audit.rule_catalog rc USING (rule_code)
                          WHERE f.run_id = ? AND (f.supplier_stir = ? OR f.entity_id LIKE '%' || ? || '%')
                          ORDER BY f.amount_at_risk DESC LIMIT 15""", [run, stir, stir]).fetchall()
    _print_findings(rows)
    return True


def inspect(settings: Settings, key: str) -> int:
    key = key.strip().upper()
    con = connect(settings, read_only=True)
    try:
        if key.startswith("P"):  # to'lov ID — tegishli shartnomani ochamiz
            row = con.execute("SELECT contract_id FROM dwh.fact_payment WHERE payment_id = ?", [key]).fetchone()
            ok = bool(row) and inspect_contract(con, row[0])
            if ok:
                print(f"  (to'lov {key} shu shartnomaga tegishli)")
        elif key.startswith("C"):
            ok = inspect_contract(con, key)
        else:
            ok = inspect_supplier(con, key)
    finally:
        con.close()
    if not ok:
        print(f"'{key}' topilmadi. Shartnoma (C0438999), to'lov (P01437186) yoki 9 xonali STIR kiriting.")
        return 1
    print()
    return 0
