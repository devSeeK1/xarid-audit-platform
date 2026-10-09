"""Yagona logging sozlamasi: konsol + fayl (logs/pipeline.log)."""
from __future__ import annotations

import logging
from pathlib import Path

FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def setup_logging(log_dir: Path, level: int = logging.INFO) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(FORMAT, "%H:%M:%S"))
    file = logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")
    file.setFormatter(logging.Formatter(FORMAT))
    root.addHandler(console)
    root.addHandler(file)
    for noisy in ("httpx", "httpcore", "py4j", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
