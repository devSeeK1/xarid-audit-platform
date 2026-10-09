"""Sozlamalarni yuklash: config/settings.yaml + muhit o'zgaruvchilari (.env)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


@dataclass
class Paths:
    data: Path

    @property
    def sources(self) -> Path:
        return self.data / "sources"

    @property
    def bronze(self) -> Path:
        return self.data / "bronze"

    @property
    def silver(self) -> Path:
        return self.data / "silver"

    @property
    def quarantine(self) -> Path:
        return self.data / "quarantine"

    @property
    def marts(self) -> Path:
        return self.data / "marts"

    @property
    def warehouse(self) -> Path:
        return self.data / "warehouse" / "xarid_dwh.duckdb"

    @property
    def reports(self) -> Path:
        return self.data.parent / "reports" if self.data.name == "data" else self.data / "reports"

    @property
    def logs(self) -> Path:
        return self.data.parent / "logs" if self.data.name == "data" else self.data / "logs"

    def ensure(self) -> None:
        for p in (self.sources, self.bronze, self.silver, self.quarantine, self.marts,
                  self.warehouse.parent, self.reports, self.logs):
            p.mkdir(parents=True, exist_ok=True)


@dataclass
class Settings:
    raw: dict[str, Any]
    paths: Paths
    api_token: str = field(repr=False, default="")
    pii_salt: str = field(repr=False, default="")

    @property
    def seed(self) -> int:
        return int(self.raw["seed"])

    @property
    def scale(self) -> dict[str, Any]:
        return self.raw["scale"]

    @property
    def period(self) -> dict[str, str]:
        return self.raw["period"]

    @property
    def thresholds(self) -> dict[str, float]:
        return self.raw["thresholds"]

    @property
    def rule_weights(self) -> dict[str, int]:
        return self.raw["rule_weights"]

    @property
    def api(self) -> dict[str, Any]:
        cfg = dict(self.raw["api"])
        cfg["port"] = int(os.getenv("TREASURY_API_PORT", cfg["port"]))
        return cfg

    @property
    def api_base_url(self) -> str:
        return os.getenv("TREASURY_API_URL", f"http://{self.api['host']}:{self.api['port']}")


def load_settings(config_path: Path | None = None) -> Settings:
    config_path = config_path or ROOT / "config" / "settings.yaml"
    with open(config_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    data_dir = Path(os.getenv("XARID_DATA_DIR", ROOT / "data")).resolve()
    token = os.getenv("TREASURY_API_TOKEN", "")
    salt = os.getenv("PII_SALT", "")
    if not token or not salt:
        raise RuntimeError(
            "TREASURY_API_TOKEN va PII_SALT muhit o'zgaruvchilari kerak. "
            ".env.example faylidan nusxa olib, .env yarating."
        )
    return Settings(raw=raw, paths=Paths(data_dir), api_token=token, pii_salt=salt)
