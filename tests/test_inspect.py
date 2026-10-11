"""`tekshir` buyrug'i: shartnoma, to'lov va yetkazuvchi bo'yicha ish yig'majildi."""
import json

from pipeline.inspect import inspect


def test_inspect_contract_shows_rule(built, settings, capsys):
    truth = json.loads((settings.paths.sources / "_ground_truth.json").read_text(encoding="utf-8"))
    cid = truth["R03_price_contracts"][0]
    assert inspect(settings, cid) == 0
    out = capsys.readouterr().out
    assert f"SHARTNOMA {cid}" in out and "[R03]" in out and "bozordan sezilarli qimmat" in out


def test_inspect_payment_and_supplier(built, settings, capsys):
    truth = json.loads((settings.paths.sources / "_ground_truth.json").read_text(encoding="utf-8"))
    assert inspect(settings, truth["R07_early_payments"][0]) == 0
    assert "shartnomadan OLDIN" in capsys.readouterr().out
    cluster = truth["R05_affiliated_clusters"][0]["stirs"]
    assert inspect(settings, str(cluster[0])) == 0
    out = capsys.readouterr().out
    assert "[R05]" in out and str(cluster[1]) in out


def test_inspect_unknown(built, settings, capsys):
    assert inspect(settings, "C9999999") == 1
