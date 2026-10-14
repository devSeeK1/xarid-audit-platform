"""Audit hisobotini (HTML) qurish: DWH dan ma'lumot olib, Jinja2 shablonini to'ldiradi.

Grafiklar tashqi kutubxonasiz, Python'da masshtab bo'yicha hisoblanadigan SVG
sifatida yaratiladi — hisobot oflayn ochiladi va bitta fayldan iborat.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from html import escape
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from pipeline.config import Settings
from pipeline.load.warehouse import connect

log = logging.getLogger(__name__)
TEMPLATE_DIR = Path(__file__).parent


# ------------------------------------------------------------ formatlash
def fmt_int(n) -> str:
    return f"{int(n):,}".replace(",", " ")


def fmt_som(x) -> str:
    """So'mni o'qishga qulay ko'rinishda: mlrd / mln."""
    x = float(x or 0)
    if abs(x) >= 1e9:
        return f"{x / 1e9:,.1f}".replace(",", " ").replace(".", ",") + " mlrd so'm"
    if abs(x) >= 1e6:
        return f"{x / 1e6:,.1f}".replace(",", " ").replace(".", ",") + " mln so'm"
    return fmt_int(x) + " so'm"


def fmt_dec(x, digits=1) -> str:
    return f"{x:,.{digits}f}".replace(",", " ").replace(".", ",")


def nice_ticks(vmax: float, n: int = 4) -> list[float]:
    if vmax <= 0:
        return [0, 1]
    raw = vmax / n
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    top = math.ceil(vmax / step) * step
    return [round(i * step, 10) for i in range(int(round(top / step)) + 1)]


# ------------------------------------------------------------ SVG grafiklar
def svg_hbar(items: list[tuple[str, float, str]], unit_fmt, tick_fmt, label_w: int = 250) -> str:
    """Gorizontal ustunlar. items: (yorliq, qiymat, tooltip)."""
    bar, gap, top, bottom, right = 18, 12, 8, 26, 120
    width = 760
    plot_w = width - label_w - right
    height = top + len(items) * (bar + gap) + bottom
    vmax = max((v for _, v, _ in items), default=1) or 1
    ticks = nice_ticks(vmax)
    scale = plot_w / ticks[-1]
    out = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img" '
           f'preserveAspectRatio="xMinYMin meet">']
    for t in ticks:
        x = label_w + t * scale
        out.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{top - 4}" y2="{height - bottom}" class="grid"/>')
        out.append(f'<text x="{x:.1f}" y="{height - 8}" class="tick" text-anchor="middle">{escape(tick_fmt(t))}</text>')
    for i, (label, v, tip) in enumerate(items):
        y = top + i * (bar + gap)
        w = max(v * scale, 1.5)
        r = min(4, w / 2)
        # 4px yumaloq uch, asos to'g'ri burchakli
        path = (f"M{label_w},{y} h{w - r:.1f} a{r},{r} 0 0 1 {r},{r} v{bar - 2 * r} "
                f"a{r},{r} 0 0 1 -{r},{r} h-{w - r:.1f} z")
        out.append(f'<g class="mark"><title>{escape(tip)}</title>'
                   f'<rect x="0" y="{y - gap / 2}" width="{width}" height="{bar + gap}" class="hit"/>'
                   f'<path d="{path}" class="bar"/>'
                   f'<text x="{label_w - 10}" y="{y + bar / 2}" class="lab" text-anchor="end" '
                   f'dominant-baseline="central">{escape(label)}</text>'
                   f'<text x="{label_w + w + 8:.1f}" y="{y + bar / 2}" class="val" '
                   f'dominant-baseline="central">{escape(unit_fmt(v))}</text></g>')
    out.append(f'<line x1="{label_w}" x2="{label_w}" y1="{top - 4}" y2="{height - bottom}" class="axis"/>')
    out.append("</svg>")
    return "".join(out)


def svg_line(points: list[tuple[str, float]], tick_fmt, tip_fmt) -> str:
    """Oylik dinamika: chiziq + yengil maydon, oxirgi nuqta ta'kidlangan, hover uchun ma'lumot."""
    width, height = 760, 260
    left, right, top, bottom = 64, 24, 16, 34
    pw, ph = width - left - right, height - top - bottom
    vmax = max(v for _, v in points) or 1
    ticks = nice_ticks(vmax)
    sy = ph / ticks[-1]
    n = len(points)
    xs = [left + (i * pw / (n - 1) if n > 1 else pw / 2) for i in range(n)]
    ys = [top + ph - v * sy for _, v in points]
    out = [f'<svg viewBox="0 0 {width} {height}" class="chart line-chart" role="img" '
           f'data-points=\'{escape(json.dumps([{"x": round(x, 1), "y": round(y, 1), "t": tip_fmt(m, v)} for (m, v), x, y in zip(points, xs, ys)]))}\'>']
    for t in ticks:
        y = top + ph - t * sy
        out.append(f'<line x1="{left}" x2="{width - right}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>')
        out.append(f'<text x="{left - 8}" y="{y:.1f}" class="tick" text-anchor="end" dominant-baseline="central">'
                   f'{escape(tick_fmt(t))}</text>')
    for i, (m, _) in enumerate(points):
        if i % 3 == 0 or i == n - 1:
            out.append(f'<text x="{xs[i]:.1f}" y="{height - 10}" class="tick" text-anchor="middle">{escape(m)}</text>')
    line = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}" for i, (x, y) in enumerate(zip(xs, ys)))
    area = line + f" L{xs[-1]:.1f},{top + ph} L{xs[0]:.1f},{top + ph} Z"
    out.append(f'<path d="{area}" class="area"/>')
    out.append(f'<path d="{line}" class="line"/>')
    out.append(f'<circle cx="{xs[-1]:.1f}" cy="{ys[-1]:.1f}" r="4.5" class="dot"/>')
    out.append(f'<line class="xhair" x1="0" x2="0" y1="{top}" y2="{top + ph}" visibility="hidden"/>')
    out.append('<circle class="xdot" r="4.5" cx="0" cy="0" visibility="hidden"/>')
    out.append(f'<rect class="hitarea" x="{left}" y="{top}" width="{pw}" height="{ph}"/>')
    out.append(f'<line x1="{left}" x2="{width - right}" y1="{top + ph}" y2="{top + ph}" class="axis"/>')
    out.append("</svg>")
    return "".join(out)


# ------------------------------------------------------------ tafsilotlar
DETAIL_LABELS = {
    "category": "toifa", "window_start": "boshlanish", "window_end": "tugash", "contracts": "shartnomalar",
    "total": "jami", "limit": "chegara", "contract_amount": "shartnoma", "paid": "to'langan",
    "overpaid_pct": "ortiqcha %", "payments": "to'lovlar", "unit_price": "birlik narxi",
    "market_median": "bozor medianasi", "ratio": "nisbat", "supplier_reg_date": "ro'yxatdan o'tgan",
    "contract_date": "shartnoma sanasi", "days_since_registration": "kun o'tgan", "amount": "summa",
    "link_type": "bog'liqlik", "suppliers": "korxonalar", "year": "yil", "share_pct": "ulush %",
    "supplier_contracts": "yetkazuvchi shartnomalari", "buyer_contracts": "buyurtmachi shartnomalari",
    "contract_id": "shartnoma", "payment_date": "to'lov sanasi", "days_before": "necha kun oldin",
    "liquidation_date": "tugatilgan sana",
}
MONEY_KEYS = {"total", "limit", "contract_amount", "paid", "unit_price", "market_median", "amount"}


def humanize_details(raw: str) -> list[tuple[str, str]]:
    d = json.loads(raw)
    out = []
    for k, v in d.items():
        if k in MONEY_KEYS and isinstance(v, (int, float)):
            v = fmt_som(v)
        elif isinstance(v, list):
            v = ", ".join(map(str, v))
        elif isinstance(v, float):
            v = fmt_dec(v, 2 if k == "ratio" else 1)
        out.append((DETAIL_LABELS.get(k, k), str(v)))
    return out


# ------------------------------------------------------------ asosiy
def collect(con, run_id: str) -> dict:
    q = lambda sql, p=None: con.execute(sql, p or []).fetchall()  # noqa: E731
    one = lambda sql, p=None: con.execute(sql, p or []).fetchone()  # noqa: E731

    run = one("SELECT run_id, started_at, finished_at, status, run_by, host, mode, duration_sec "
              "FROM meta.etl_runs WHERE run_id = ?", [run_id])
    contracts = one("SELECT COUNT(*), SUM(amount), MIN(contract_date), MAX(contract_date) FROM dwh.fact_contract")
    payments = one("SELECT COUNT(*), SUM(amount) FROM dwh.fact_payment")
    stats = q("SELECT dataset, rows_in, rows_out, rows_quarantined FROM meta.dataset_stats WHERE run_id = ? "
              "ORDER BY rows_in DESC", [run_id])
    rules = q("""SELECT rc.rule_code, rc.name, rc.description, rc.severity, rc.weight,
                        COUNT(f.rule_code), COALESCE(SUM(f.amount_at_risk), 0)
                 FROM audit.rule_catalog rc
                 LEFT JOIN audit.findings f ON f.rule_code = rc.rule_code AND f.run_id = ?
                 GROUP BY ALL ORDER BY rc.rule_code""", [run_id])
    total_findings = sum(r[5] for r in rules)
    total_risk = sum(r[6] for r in rules)
    buyers = q("SELECT buyer_id, buyer_name, region, sector, findings, risk_score, amount_at_risk, rules "
               "FROM audit.buyer_risk LIMIT 12")
    regions = q("""SELECT b.region, SUM(f.amount_at_risk) AS amt, COUNT(*) AS n
                   FROM audit.findings f JOIN dwh.dim_buyer b USING (buyer_id)
                   WHERE f.run_id = ? GROUP BY ALL ORDER BY amt DESC""", [run_id])
    monthly = q("""SELECT strftime(date_trunc('month', contract_date), '%Y-%m') AS m, SUM(amount), COUNT(*)
                   FROM dwh.fact_contract GROUP BY 1 ORDER BY 1""")
    samples = {}
    for code, *_ in rules:
        samples[code] = [
            {"entity": e, "buyer": b, "stir": s, "amount": fmt_som(a), "details": humanize_details(d)}
            for e, b, s, a, d in q("""SELECT entity_id, buyer_id, supplier_stir, amount_at_risk, details
                                       FROM audit.findings WHERE run_id = ? AND rule_code = ?
                                       ORDER BY amount_at_risk DESC LIMIT 3""", [run_id, code])]
    checks = q("""SELECT dataset, check_name, failed_rows FROM meta.dq_results
                  WHERE run_id = ? AND failed_rows > 0 AND dataset <> 'spark_reconciliation'
                  ORDER BY dataset, failed_rows DESC""", [run_id])
    recon = q("SELECT check_name, failed_rows FROM meta.dq_results WHERE run_id = ? AND dataset = 'spark_reconciliation'",
              [run_id])
    lineage = q("SELECT source, location, sha256, rows FROM meta.source_lineage WHERE run_id = ?", [run_id])
    timings = q("SELECT step, seconds FROM meta.step_timings WHERE run_id = ? ORDER BY step", [run_id])

    rows_total = sum(s[1] for s in stats)
    quarantined = sum(s[3] for s in stats)
    return dict(
        run=dict(id=run[0], started=run[1].strftime("%d.%m.%Y %H:%M"), status=run[3], by=run[4], host=run[5],
                 mode=run[6], duration=fmt_dec(run[7] or 0)),
        kpi=[
            ("Shartnomalar", fmt_int(contracts[0]), fmt_som(contracts[1])),
            ("To'lovlar", fmt_int(payments[0]), fmt_som(payments[1])),
            ("Qayta ishlangan qatorlar", fmt_int(rows_total), f"karantinda {fmt_int(quarantined)}"),
            ("Audit topilmalari", fmt_int(total_findings), f"{len([r for r in rules if r[5]])} ta qoida bo'yicha"),
        ],
        hero_risk=fmt_som(total_risk),
        period=f"{contracts[2]:%d.%m.%Y} – {contracts[3]:%d.%m.%Y}",
        rules=[dict(code=r[0], name=r[1], desc=r[2], severity=r[3], weight=r[4], n=fmt_int(r[5]),
                    amount=fmt_som(r[6]), samples=samples[r[0]]) for r in rules],
        chart_rules=svg_hbar([(f"{r[0]} · {r[1]}", r[5], f"{r[0]} {r[1]}: {fmt_int(r[5])} ta, {fmt_som(r[6])}")
                              for r in rules], fmt_int, lambda t: fmt_int(t), label_w=310),
        chart_regions=svg_hbar([(r[0], r[1] / 1e9, f"{r[0]}: {fmt_som(r[1])}, {fmt_int(r[2])} ta topilma")
                                for r in regions], lambda v: f"{fmt_dec(v)} mlrd", lambda t: fmt_dec(t, 0),
                               label_w=220),
        chart_monthly=svg_line([(m, v / 1e9) for m, v, _ in monthly], lambda t: fmt_dec(t, 0),
                               lambda m, v: f"{m}: {fmt_dec(v)} mlrd so'm"),
        buyers=[dict(id=b[0], name=b[1], region=b[2], sector=b[3], findings=fmt_int(b[4]), score=b[5],
                     amount=fmt_som(b[6]), rules=b[7]) for b in buyers],
        stats=[dict(name=s[0], rows_in=fmt_int(s[1]), rows_out=fmt_int(s[2]), quarantined=fmt_int(s[3]),
                    pct=fmt_dec(100 * s[2] / s[1] if s[1] else 100, 2)) for s in stats],
        checks=[dict(dataset=c[0], name=c[1], n=fmt_int(c[2])) for c in checks],
        recon=[dict(name=c[0], n=c[1]) for c in recon],
        lineage=[dict(source=l[0], location=l[1], sha=l[2], sha_short=l[2][:16], rows=fmt_int(l[3])) for l in lineage],
        timings=[dict(step=t[0], sec=fmt_dec(t[1], 2)) for t in timings],
        generated=datetime.now().strftime("%d.%m.%Y %H:%M"),
    )


def build_report(settings: Settings, run_id: str | None = None, standalone: bool = True,
                 out_path: Path | None = None) -> Path:
    con = connect(settings, read_only=True)
    try:
        if run_id is None:
            run_id = con.execute("SELECT run_id FROM meta.etl_runs WHERE status = 'success' "
                                 "ORDER BY started_at DESC LIMIT 1").fetchone()[0]
        ctx = collect(con, run_id)
    finally:
        con.close()
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=select_autoescape(["html", "j2"]))
    html = env.get_template("template.html.j2").render(**ctx, standalone=standalone)
    out_path = out_path or settings.paths.reports / "audit_report.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    log.info("HISOBOT %s", out_path)
    return out_path
