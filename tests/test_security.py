import pytest
from fastapi.testclient import TestClient

from pipeline.audit.engine import render
from pipeline.transform.clean import mask_pii


def test_mask_pii_is_deterministic_and_irreversible():
    a, b = mask_pii("31234567890123", "salt"), mask_pii("31234567890123", "salt")
    assert a == b and len(a) == 16 and "31234567890123" not in a
    assert mask_pii("31234567890123", "other-salt") != a  # tuzsiz lug'at hujumi ishlamaydi
    assert mask_pii(None, "salt") is None


def test_sql_params_must_be_numeric():
    assert render("x > {a}", {"a": 5}) == "x > 5"
    with pytest.raises(ValueError):
        render("x > {a}", {"a": "1; DROP TABLE dwh.fact_contract"})
    with pytest.raises(KeyError):
        render("x > {b}", {"a": 1})


def test_api_requires_token(settings):
    from mock_api.app import app
    client = TestClient(app)
    assert client.get("/api/v1/payments").status_code == 401
    assert client.get("/api/v1/payments", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.get("/health").status_code == 200


def test_api_pagination(built, settings):
    from mock_api.app import app
    client = TestClient(app)
    h = {"Authorization": f"Bearer {settings.api_token}"}
    r1 = client.get("/api/v1/payments", params={"page": 1, "page_size": 1000}, headers=h).json()
    r2 = client.get("/api/v1/payments", params={"page": 2, "page_size": 1000}, headers=h).json()
    assert len(r1["items"]) == 1000 and r1["pages"] == -(-r1["total"] // 1000)
    assert {i["payment_id"] for i in r1["items"]}.isdisjoint({i["payment_id"] for i in r2["items"]})
