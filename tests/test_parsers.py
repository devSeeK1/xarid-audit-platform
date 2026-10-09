from datetime import date

import polars as pl
import pytest

from pipeline.transform.clean import normalize_method, parse_amount, parse_date, valid_stir


@pytest.mark.parametrize("raw, expected", [
    ("1250000.50", 1250000.5),
    ("1 250 000,50", 1250000.5),
    ("1,250,000.50", 1250000.5),
    ("  987654,1 ", 987654.1),
    ("-1 000,00", -1000.0),
    ("12", 12.0),
])
def test_parse_amount_formats(raw, expected):
    out = pl.DataFrame({"a": [raw]}).select(parse_amount("a")).item()
    assert out == pytest.approx(expected)


def test_parse_amount_garbage_is_null():
    assert pl.DataFrame({"a": ["abc"]}).select(parse_amount("a")).item() is None


@pytest.mark.parametrize("raw, expected", [
    ("2024-03-15", date(2024, 3, 15)),
    ("15.03.2024", date(2024, 3, 15)),
    (" 01.12.2025 ", date(2025, 12, 1)),
    ("2024/03/15", None),
])
def test_parse_date(raw, expected):
    assert pl.DataFrame({"d": [raw]}).select(parse_date("d")).item() == expected


def test_normalize_method():
    df = pl.DataFrame({"m": ["  TENDER ", "Elektron_dokon", "togridan togri  "]})
    assert df.select(normalize_method("m")).to_series().to_list() == ["tender", "elektron_dokon", "togridan_togri"]


def test_valid_stir():
    df = pl.DataFrame({"s": ["912345678", "12345678", "9XX0OO123", None]})
    assert df.select(valid_stir("s")).to_series().to_list() == [True, False, False, False]
