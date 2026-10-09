"""Sintetik manba tizimlarini yaratish.

Real davlat tizimlarining ma'lumotlari ochiq emas, shuning uchun platforma
SINTETIK (o'ylab topilgan) ma'lumotlar bilan namoyish qilinadi. Barcha tashkilot
nomlari, STIR (9 bilan boshlanadi) va PINFL raqamlari tasodifiy yaratilgan va
hech qanday real shaxs yoki tashkilotga tegishli emas.

Manba tizimlari (har biri alohida format — real hayotdagidek):
  * xarid_portal/contracts.csv      — xarid portali eksporti (;-ajratilgan, "iflos")
  * soliq_registry/suppliers.xlsx   — yetkazuvchilar reyestri (Excel)
  * treasury/payments.parquet       — g'aznachilik to'lovlari (faqat API orqali beriladi)
  * statistika/market_prices.csv    — bozor narxlari indeksi
  * reference/buyers.csv            — buyurtmachi tashkilotlar ma'lumotnomasi

Audit qoidalarini sinash uchun ma'lumotlarga ataylab "anomaliyalar" qo'shiladi
va ularning ro'yxati _ground_truth.json ga yoziladi (faqat testlar uchun).
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta

import numpy as np
import polars as pl

from pipeline.config import Settings

log = logging.getLogger(__name__)

REGIONS = [
    "Toshkent shahri", "Toshkent viloyati", "Andijon", "Buxoro", "Farg'ona", "Jizzax",
    "Xorazm", "Namangan", "Navoiy", "Qashqadaryo", "Samarqand", "Sirdaryo",
    "Surxondaryo", "Qoraqalpog'iston Respublikasi",
]
SECTORS = ["Sog'liqni saqlash", "Xalq ta'limi", "Oliy ta'lim", "Qurilish va kommunal",
           "Qishloq xo'jaligi", "Transport", "Madaniyat va sport", "Ijtimoiy himoya"]
BUDGET_TYPES = ["Respublika byudjeti", "Mahalliy byudjet", "Byudjetdan tashqari jamg'arma"]
# (toifa, o'lchov birligi, bazaviy birlik narxi so'mda, nisbiy ulush)
CATEGORIES = [
    ("Kompyuter texnikasi", "dona", 9_500_000, 0.10),
    ("Ofis mebeli", "dona", 2_300_000, 0.09),
    ("Dori-darmon", "qadoq", 85_000, 0.14),
    ("Tibbiy jihozlar", "dona", 14_000_000, 0.05),
    ("Qurilish materiallari", "tonna", 1_900_000, 0.12),
    ("Oziq-ovqat mahsulotlari", "kg", 32_000, 0.16),
    ("Yoqilg'i", "litr", 11_500, 0.11),
    ("Kanselyariya", "qadoq", 45_000, 0.10),
    ("Avtomobil ehtiyot qismlari", "dona", 650_000, 0.07),
    ("IT xizmatlari", "soat", 350_000, 0.06),
]
METHODS_SMALL = (["togridan_togri", "elektron_dokon", "auksion"], [0.45, 0.45, 0.10])
METHODS_LARGE = (["tender", "auksion"], [0.6, 0.4])
EPOCH = np.datetime64("1970-01-01", "D")


def _d(s: str) -> np.datetime64:
    return np.datetime64(s, "D")


def _scaled(n: int, scale: float, minimum: int = 3) -> int:
    return max(minimum, int(round(n * scale)))


def generate(settings: Settings, scale: float = 1.0) -> dict:
    rng = np.random.default_rng(settings.seed)
    sc = settings.scale
    th = settings.thresholds
    limit = float(th["direct_purchase_limit"])
    p_start, p_end = _d(settings.period["start"]), _d(settings.period["end"])
    n_days = int((p_end - p_start).astype(int)) + 1
    src = settings.paths.sources
    for sub in ("xarid_portal", "soliq_registry", "treasury", "statistika", "reference"):
        (src / sub).mkdir(parents=True, exist_ok=True)

    n_b = _scaled(sc["buyers"], scale, 40)
    n_s = _scaled(sc["suppliers"], scale, 600)
    n_c = _scaled(sc["contracts"], scale, 5000)
    truth: dict[str, list] = {}

    # ------------------------------------------------------------------ buyers
    buyer_ids = np.array([f"B{i:04d}" for i in range(1, n_b + 1)])
    buyers = pl.DataFrame({
        "buyer_id": buyer_ids,
        "buyer_name": [f"Buyurtmachi tashkilot {b}" for b in buyer_ids],
        "sector": rng.choice(SECTORS, n_b),
        "region": rng.choice(REGIONS, n_b),
        "budget_type": rng.choice(BUDGET_TYPES, n_b, p=[0.5, 0.4, 0.1]),
    })
    buyer_w = 1.0 / np.arange(1, n_b + 1) ** 0.7
    buyer_w = rng.permutation(buyer_w / buyer_w.sum())

    # --------------------------------------------------------------- suppliers
    reserved = max(60, int(n_s * 0.04))  # anomaliya ssenariylari uchun ajratilgan
    n_regular = n_s - reserved
    stirs = (900_000_000 + rng.choice(99_999_999, size=n_s, replace=False)).astype(np.int64)
    old_start, old_end = _d("2010-01-01"), _d("2023-06-30")
    reg = old_start + rng.integers(0, int((old_end - old_start).astype(int)), n_s).astype("timedelta64[D]")
    new_mask = rng.random(n_regular) < 0.10
    reg[:n_regular][new_mask] = _d("2023-07-01") + rng.integers(0, 640, new_mask.sum()).astype("timedelta64[D]")
    pinfl = np.array([f"3{x:013d}" for x in rng.choice(10**13 - 1, size=n_s, replace=False)])
    s_region = rng.choice(REGIONS, n_s)
    address = np.array([f"{r}, Sintetik ko'chasi, {i + 1}-bino" for i, r in enumerate(s_region)], dtype=object)
    status = np.array(["faol"] * n_s, dtype=object)
    liq_date = np.full(n_s, np.datetime64("NaT", "D"), dtype="datetime64[D]")
    supp_w = rng.pareto(1.6, n_regular) + 0.05
    supp_w = supp_w / supp_w.sum()

    cat_names = np.array([c[0] for c in CATEGORIES])
    cat_base = np.array([c[2] for c in CATEGORIES], dtype=float)
    cat_p = np.array([c[3] for c in CATEGORIES])
    cat_p = cat_p / cat_p.sum()

    # Bozor narxlari: oylik ~0.9% inflyatsiya + toifa bo'yicha mavsumiylik
    months = np.arange(np.datetime64(settings.period["start"][:7], "M"),
                       np.datetime64(settings.period["end"][:7], "M") + np.timedelta64(1, "M"))
    n_m = len(months)
    month_idx = np.arange(n_m)
    market = np.empty((len(CATEGORIES), n_m))
    for k in range(len(CATEGORIES)):
        season = 1 + 0.04 * np.sin(2 * np.pi * (month_idx + k) / 12)
        market[k] = cat_base[k] * (1.009 ** month_idx) * season

    def month_of(dates: np.ndarray) -> np.ndarray:
        return (dates.astype("datetime64[M]") - months[0]).astype(int)

    # --------------------------------------------------------- base contracts
    c_buyer = rng.choice(n_b, n_c, p=buyer_w)
    c_supp = rng.choice(n_regular, n_c, p=supp_w)
    c_date = p_start + rng.integers(0, n_days, n_c).astype("timedelta64[D]")
    # yetkazuvchi shartnomadan kamida 120 kun oldin ro'yxatdan o'tgan bo'lishi kerak
    bad = reg[c_supp] + np.timedelta64(120, "D") > c_date
    old_pool = np.where(~new_mask)[0]
    c_supp[bad] = rng.choice(old_pool, bad.sum())
    c_cat = rng.choice(len(CATEGORIES), n_c, p=cat_p)
    c_price = market[c_cat, month_of(c_date)] * rng.lognormal(0, 0.12, n_c)
    target_amount = np.exp(rng.normal(np.log(18_000_000), 1.35, n_c))
    c_qty = np.maximum(1, np.round(target_amount / c_price))
    c_price = np.round(c_price, 2)
    c_amount = np.round(c_qty * c_price, 2)
    c_method = np.empty(n_c, dtype=object)
    small = c_amount < limit
    c_method[small] = rng.choice(METHODS_SMALL[0], small.sum(), p=METHODS_SMALL[1])
    c_method[~small] = rng.choice(METHODS_LARGE[0], (~small).sum(), p=METHODS_LARGE[1])

    extra: list[dict] = []  # anomaliya shartnomalari

    def add_contract(b, s, d, cat, amount, method, floor=False):
        price = round(float(market[cat, month_of(np.array([d]))[0]] * rng.lognormal(0, 0.05)), 2)
        qty = max(1.0, float(np.floor(amount / price)) if floor else round(amount / price))
        extra.append(dict(b=b, s=s, d=d, cat=cat, qty=qty, price=price,
                          amount=round(qty * price, 2), method=method))

    # R01: bo'lib xarid qilish — chegaradan sal pastroq ketma-ket to'g'ridan-to'g'ri shartnomalar
    split_groups = []
    for _ in range(_scaled(60, scale)):
        cheap = np.where(cat_base <= 2_500_000)[0]  # chegaraga yaqin summani aniq tuzish uchun
        b, s, cat = rng.integers(n_b), int(rng.choice(old_pool)), int(rng.choice(cheap))
        d0 = p_start + np.timedelta64(int(rng.integers(0, n_days - 40)), "D")
        k = int(rng.integers(3, 7))
        for off in np.sort(rng.integers(0, 26, k)):
            add_contract(b, s, d0 + np.timedelta64(int(off), "D"), cat, limit * rng.uniform(0.86, 0.97),
                         "togridan_togri", floor=True)
        split_groups.append([str(buyer_ids[b]), int(stirs[s]), str(cat_names[cat])])
    truth["R01_split_groups"] = split_groups

    # R04: yangi tashkil etilgan korxonaga yirik shartnoma
    res = np.arange(n_regular, n_s)
    rng.shuffle(res)
    new_cos, res = res[: _scaled(80, scale)], res[_scaled(80, scale):]
    r04_stirs = []
    for s in new_cos:
        d_reg = _d("2024-03-01") + np.timedelta64(int(rng.integers(0, 560)), "D")
        reg[s] = d_reg
        for _ in range(int(rng.integers(1, 3))):
            add_contract(int(rng.integers(n_b)), int(s), d_reg + np.timedelta64(int(rng.integers(10, 60)), "D"),
                         int(rng.integers(len(CATEGORIES))), rng.uniform(5.5e8, 3e9),
                         str(rng.choice(["tender", "auksion"])))
        r04_stirs.append(int(stirs[s]))
    truth["R04_new_company_stirs"] = r04_stirs

    # R05: affillangan yetkazuvchilar — bitta direktor/manzil, bitta buyurtmachida g'olib
    clusters = []
    for _ in range(_scaled(40, scale)):
        k = int(rng.integers(2, 5))
        if len(res) < k:
            break
        members, res = res[:k], res[k:]
        shared_pinfl, shared_addr = pinfl[members[0]], address[members[0]]
        pinfl[members] = shared_pinfl
        address[members] = shared_addr
        b = int(rng.integers(n_b))
        for m in members:
            reg[m] = old_start + np.timedelta64(int(rng.integers(0, 3000)), "D")
        for i in range(int(rng.integers(6, 15))):
            m = members[i % k]
            add_contract(b, int(m), p_start + np.timedelta64(int(rng.integers(0, n_days)), "D"),
                         int(rng.integers(len(CATEGORIES))), limit * rng.uniform(2, 20), "tender")
        clusters.append({"buyer_id": str(buyer_ids[b]), "stirs": [int(stirs[m]) for m in members]})
    truth["R05_affiliated_clusters"] = clusters

    # R06: konsentratsiya — bitta yetkazuvchi buyurtmachi yillik xarajatining >50% ini oladi
    year = c_date.astype("datetime64[Y]").astype(int) + 1970
    conc = []
    big_buyers = np.argsort(-buyer_w)[: max(10, n_b // 4)]
    for b in rng.choice(big_buyers, min(_scaled(25, scale), len(big_buyers)), replace=False):
        y = int(rng.choice([2024, 2025]))
        mask = (c_buyer == b) & (year == y)
        spend = c_amount[mask].sum()
        if mask.sum() < th["concentration_min_contracts"]:
            continue
        s = int(rng.choice(old_pool))
        target = spend * rng.uniform(1.4, 2.2)
        n_new = int(rng.integers(8, 20))
        for amt in rng.dirichlet(np.ones(n_new)) * target:
            add_contract(int(b), s, np.datetime64(f"{y}-01-01") + np.timedelta64(int(rng.integers(0, 365)), "D"),
                         int(rng.integers(len(CATEGORIES))), max(amt, limit * 1.05), "tender")
        conc.append([str(buyer_ids[b]), y, int(stirs[s])])
    truth["R06_concentration"] = conc

    # anomaliya shartnomalarini qo'shamiz
    if extra:
        e = pl.DataFrame(extra)
        c_buyer = np.concatenate([c_buyer, e["b"].to_numpy()])
        c_supp = np.concatenate([c_supp, e["s"].to_numpy()])
        c_date = np.concatenate([c_date, np.array(e["d"].to_list(), dtype="datetime64[D]")])
        c_cat = np.concatenate([c_cat, e["cat"].to_numpy()])
        c_qty = np.concatenate([c_qty, e["qty"].to_numpy()])
        c_price = np.concatenate([c_price, e["price"].to_numpy()])
        c_amount = np.concatenate([c_amount, e["amount"].to_numpy()])
        c_method = np.concatenate([c_method, np.array(e["method"].to_list(), dtype=object)])
    n_total = len(c_buyer)
    order = np.argsort(c_date, kind="stable")
    c_buyer, c_supp, c_date, c_cat = c_buyer[order], c_supp[order], c_date[order], c_cat[order]
    c_qty, c_price, c_amount, c_method = c_qty[order], c_price[order], c_amount[order], c_method[order]
    contract_ids = np.array([f"C{i:07d}" for i in range(1, n_total + 1)])
    is_anomaly = np.zeros(n_total, dtype=bool)
    is_anomaly[np.isin(order, np.arange(n_c, n_total))] = True

    # R03: narxni sun'iy oshirish (oddiy shartnomalardan tanlanadi)
    normal_idx = np.where(~is_anomaly)[0]
    r03 = rng.choice(normal_idx, _scaled(500, scale), replace=False)
    c_price[r03] = np.round(c_price[r03] * rng.uniform(2.2, 4.0, len(r03)), 2)
    c_amount[r03] = np.round(c_qty[r03] * c_price[r03], 2)
    is_anomaly[r03] = True
    truth["R03_price_contracts"] = sorted(contract_ids[r03].tolist())

    # ------------------------------------------------------------- payments
    k_pay = 1 + rng.poisson(settings.scale["payments_per_contract_mean"] - 1, n_total)
    p_contract = np.repeat(np.arange(n_total), k_pay)
    w = rng.exponential(1.0, len(p_contract))
    w_sum = np.bincount(p_contract, weights=w)
    paid_frac = rng.uniform(0.85, 1.0, n_total)
    p_amount = np.round(c_amount[p_contract] * paid_frac[p_contract] * w / w_sum[p_contract], 2)
    p_date = c_date[p_contract] + rng.integers(1, 181, len(p_contract)).astype("timedelta64[D]")
    p_ids = np.array([f"P{i:08d}" for i in range(1, len(p_contract) + 1)])

    # R02: ortiqcha to'lov
    r02 = rng.choice(normal_idx[~np.isin(normal_idx, r03)], _scaled(300, scale), replace=False)
    factor = np.ones(n_total)
    factor[r02] = rng.uniform(1.08, 1.4, len(r02)) / paid_frac[r02]
    p_amount = np.round(p_amount * factor[p_contract], 2)
    truth["R02_overpaid_contracts"] = sorted(contract_ids[r02].tolist())

    # R07: shartnoma sanasidan oldingi to'lov
    first_pay = np.searchsorted(p_contract, np.arange(n_total))
    cand = np.setdiff1d(normal_idx, np.concatenate([r02, r03]))
    r07c = rng.choice(cand, _scaled(200, scale), replace=False)
    r07p = first_pay[r07c]
    p_date[r07p] = c_date[r07c] - rng.integers(5, 61, len(r07c)).astype("timedelta64[D]")
    truth["R07_early_payments"] = sorted(p_ids[r07p].tolist())

    # R08: tugatilgan (likvidatsiya qilingan) korxonaga to'lov
    supp_of_pay = c_supp[p_contract]
    last_pay = np.full(n_s, np.datetime64("NaT", "D"), dtype="datetime64[D]")
    pay_df = pl.DataFrame({"s": supp_of_pay, "d": p_date})
    agg = pay_df.group_by("s").agg(pl.col("d").max().alias("mx"), pl.col("d").min().alias("mn"))
    s_with_pay = agg["s"].to_numpy()
    last_pay[s_with_pay] = np.array(agg["mx"].to_list(), dtype="datetime64[D]")
    regular_with_pay = s_with_pay[s_with_pay < n_regular]
    regular_with_pay = np.setdiff1d(regular_with_pay, c_supp[is_anomaly])
    r08 = rng.choice(regular_with_pay, _scaled(100, scale), replace=False)
    status[r08] = "tugatilgan"
    liq_date[r08] = last_pay[r08] - rng.integers(15, 90, len(r08)).astype("timedelta64[D]")
    truth["R08_liquidated_stirs"] = sorted(int(x) for x in stirs[r08])
    # qonuniy tugatilgan korxonalar (oxirgi to'lovdan keyin) — yolg'on signal bermasligi kerak
    legit_pool = np.setdiff1d(regular_with_pay, r08)
    legit = rng.choice(legit_pool, min(len(legit_pool), _scaled(200, scale)), replace=False)
    status[legit] = "tugatilgan"
    liq_date[legit] = last_pay[legit] + rng.integers(30, 200, len(legit)).astype("timedelta64[D]")

    # ------------------------------------------------- "iflos" ma'lumotlar (contracts CSV)
    stir_str = np.array([str(x) for x in stirs[c_supp]], dtype=object)
    method_out = c_method.astype(object).copy()
    buyer_out = buyer_ids[c_buyer].astype(object)
    amount_num = c_amount.copy()
    protected = is_anomaly.copy()
    protected[r02] = True
    protected[r07c] = True
    protected[np.isin(c_supp, r08)] = True
    dirty_pool = np.where(~protected)[0]

    def pick(frac):
        return rng.choice(dirty_pool, max(1, int(n_total * frac)), replace=False)

    neg = pick(0.002)
    amount_num[neg] = -amount_num[neg]
    stir_str[pick(0.0015)] = None
    inv = pick(0.0015)
    stir_str[inv] = np.where(rng.random(len(inv)) < 0.5, "12345678", "9XX0OO123")
    buyer_out[pick(0.001)] = "B9999"
    var = pick(0.05)
    method_out[var] = [rng.choice([f"  {m.upper()} ", m.capitalize(), f"{m}  "]) for m in method_out[var]]

    fmt_choice = rng.random(n_total)
    amount_str = np.empty(n_total, dtype=object)
    for i in range(n_total):
        a = amount_num[i]
        if fmt_choice[i] < 0.70:
            amount_str[i] = f"{a:.2f}"
        elif fmt_choice[i] < 0.85:
            amount_str[i] = f"{a:,.2f}".replace(",", " ").replace(".", ",")
        else:
            amount_str[i] = f"{a:,.2f}"
    dates_pl = pl.Series(c_date)
    date_iso = dates_pl.dt.strftime("%Y-%m-%d")
    date_dmy = dates_pl.dt.strftime("%d.%m.%Y")
    use_dmy = pl.Series(rng.random(n_total) < 0.20)
    date_str = pl.select(pl.when(use_dmy).then(date_dmy).otherwise(date_iso)).to_series().to_numpy().astype(object)
    future = pick(0.0005)
    date_str[future] = "2027-05-14"

    contracts = pl.DataFrame({
        "shartnoma_id": contract_ids,
        "buyurtmachi_id": buyer_out,
        "yetkazuvchi_stir": stir_str,
        "sana": date_str,
        "xarid_usuli": method_out,
        "tovar_toifasi": cat_names[c_cat],
        "miqdor": c_qty,
        "birlik_narxi": c_price,
        "summa": amount_str,
    }, schema_overrides={"yetkazuvchi_stir": pl.Utf8, "buyurtmachi_id": pl.Utf8,
                         "sana": pl.Utf8, "xarid_usuli": pl.Utf8, "summa": pl.Utf8})
    dup_idx = rng.choice(dirty_pool, int(n_total * 0.008), replace=False)
    dup = contracts.with_row_index("_i").filter(pl.col("_i").is_in(dup_idx)).drop("_i")
    contracts = pl.concat([contracts, dup]).sample(fraction=1.0, shuffle=True, seed=settings.seed)
    csv_path = src / "xarid_portal" / "contracts_2024_2025.csv"
    with open(csv_path, "wb") as f:
        f.write("﻿".encode("utf-8"))  # 1C/Excel eksportlaridagi kabi BOM
        contracts.write_csv(f, separator=";")

    # ------------------------------------------------------- payments (treasury)
    p_contract_id = contract_ids[p_contract].astype(object)
    keep_clean = np.isin(p_contract, np.concatenate([r02, r03])) | np.isin(supp_of_pay, r08)
    pay_pool = np.setdiff1d(np.where(~keep_clean)[0], r07p)
    p_contract_id[rng.choice(pay_pool, max(1, int(len(p_ids) * 0.0005)), replace=False)] = None
    p_contract_id[rng.choice(pay_pool, max(1, int(len(p_ids) * 0.0005)), replace=False)] = "C9999999"
    payments = pl.DataFrame({
        "payment_id": p_ids,
        "contract_id": p_contract_id,
        "payment_date": p_date,
        "amount": p_amount,
        "treasury_account": [f"2340200010{x:010d}" for x in rng.integers(0, 10**10, len(p_ids))],
    }, schema_overrides={"contract_id": pl.Utf8})
    payments = pl.concat([payments, payments.sample(n=int(len(payments) * 0.005), seed=1)])  # API takrorlari
    payments.sort("payment_date").write_parquet(src / "treasury" / "payments.parquet")

    # ------------------------------------------------------- supplier registry (xlsx)
    stir_xlsx = [str(x) for x in stirs]
    for i in rng.choice(n_s, int(n_s * 0.02), replace=False):
        stir_xlsx[i] = f" {stir_xlsx[i]} "
    suppliers = pl.DataFrame({
        "STIR": stir_xlsx,
        "Nomi": [f"YT-{i + 1:05d} MChJ" + ("  " if i % 37 == 0 else "") for i in range(n_s)],
        "Ro'yxatdan o'tgan sana": reg,
        "Hudud": s_region,
        "Yuridik manzil": address.astype(str),
        "Direktor PINFL": pinfl,
        "Holati": status.astype(str),
        "Tugatilgan sana": liq_date,
    })
    suppliers.write_excel(src / "soliq_registry" / "suppliers.xlsx", worksheet="Reyestr", autofit=False)

    # ------------------------------------------------------- references
    buyers.write_csv(src / "reference" / "buyers.csv")
    mp_rows = [{"toifa": cat_names[k], "birlik": CATEGORIES[k][1], "oy": str(months[m]),
                "median_narx": round(float(market[k, m]), 2)}
               for k in range(len(CATEGORIES)) for m in range(n_m)]
    pl.DataFrame(mp_rows).write_csv(src / "statistika" / "market_prices.csv")

    truth["_note"] = "Faqat testlar uchun: ataylab qo'shilgan anomaliyalar ro'yxati."
    (src / "_ground_truth.json").write_text(json.dumps(truth, ensure_ascii=False, indent=1), encoding="utf-8")

    summary = {"buyers": n_b, "suppliers": n_s, "contracts_rows": contracts.height,
               "payments_rows": payments.height}
    log.info("Sintetik ma'lumotlar yaratildi: %s", summary)
    return summary
