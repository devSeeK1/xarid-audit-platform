"""Testlar uchun umumiy sozlama: kichik hajmdagi sintetik ma'lumotda to'liq pipeline."""
import os
import tempfile
from pathlib import Path

import pytest

_TMP = Path(tempfile.mkdtemp(prefix="xarid_test_"))
os.environ["XARID_DATA_DIR"] = str(_TMP / "data")
os.environ["TREASURY_API_PORT"] = "8799"
os.environ.setdefault("TREASURY_API_TOKEN", "test-token-0123456789")
os.environ.setdefault("PII_SALT", "test-salt-0123456789")
os.environ.pop("TREASURY_API_URL", None)


@pytest.fixture(scope="session")
def settings():
    from pipeline.config import load_settings
    s = load_settings()
    s.paths.ensure()
    return s


@pytest.fixture(scope="session")
def built(settings):
    """Ma'lumot yaratish + to'liq ishga tushirish (bir marta, butun sessiya uchun)."""
    from pipeline.generate import generate
    from pipeline.run import run_pipeline
    generate(settings, scale=0.03)
    run_id = run_pipeline(settings, full_refresh=True, start_api=True, with_spark=False, build_report=True)
    return run_id
