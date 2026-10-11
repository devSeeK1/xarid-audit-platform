"""Buyruqlar qatori interfeysi.

    python -m pipeline generate [--scale 1.0]     sintetik manba tizimlarini yaratish
    python -m pipeline api                        g'aznachilik mock API'sini ishga tushirish
    python -m pipeline run [--full-refresh] [--start-api] [--with-spark]
    python -m pipeline report [--run-id ID]       HTML audit hisobotini qayta qurish
    python -m pipeline tekshir C0438999           shartnoma yoki yetkazuvchi (STIR) bo'yicha ish yig'majildi
    python -m pipeline demo [--scale 1.0] [--with-spark]   hammasini bitta buyruqda
"""
from __future__ import annotations

import argparse
import sys

from pipeline.config import load_settings
from pipeline.logging_setup import setup_logging


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline", description="Xarid Audit Platform")
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--scale", type=float, default=1.0)
    sub.add_parser("api")
    r = sub.add_parser("run")
    r.add_argument("--full-refresh", action="store_true")
    r.add_argument("--start-api", action="store_true")
    r.add_argument("--with-spark", action="store_true")
    r.add_argument("--no-report", action="store_true")
    rp = sub.add_parser("report")
    rp.add_argument("--run-id")
    t = sub.add_parser("tekshir", help="shartnoma ID yoki yetkazuvchi STIR")
    t.add_argument("key")
    d = sub.add_parser("demo")
    d.add_argument("--scale", type=float, default=1.0)
    d.add_argument("--with-spark", action="store_true")
    args = ap.parse_args(argv)

    settings = load_settings()
    settings.paths.ensure()
    if args.cmd == "tekshir":
        from pipeline.inspect import inspect
        return inspect(settings, args.key)
    setup_logging(settings.paths.logs)

    if args.cmd == "generate":
        from pipeline.generate import generate
        generate(settings, args.scale)
    elif args.cmd == "api":
        import uvicorn
        uvicorn.run("mock_api.app:app", host=settings.api["host"], port=settings.api["port"])
    elif args.cmd == "run":
        from pipeline.run import run_pipeline
        run_pipeline(settings, args.full_refresh, args.start_api, args.with_spark, not args.no_report)
    elif args.cmd == "report":
        from pipeline.report.build import build_report
        build_report(settings, args.run_id)
    elif args.cmd == "demo":
        from pipeline.generate import generate
        from pipeline.run import run_pipeline
        generate(settings, args.scale)
        run_pipeline(settings, full_refresh=True, start_api=True, with_spark=args.with_spark)
    return 0


if __name__ == "__main__":
    sys.exit(main())
