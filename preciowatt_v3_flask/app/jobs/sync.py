from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from app.logging_config import configure_logging
from app.runtime_config import RuntimeConfig
from app.services.sync_service import SyncService


def build_runtime_config() -> RuntimeConfig:
    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv(base_dir / ".env")
    configure_logging()
    return RuntimeConfig.from_env(base_dir=base_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run PrecioWatt sync jobs")
    parser.add_argument("command", choices=["backfill", "backfill-if-needed", "daily"])
    args = parser.parse_args()

    runtime_config = build_runtime_config()
    service = SyncService(runtime_config)

    if args.command == "backfill":
        service.run_backfill()
        return 0
    if args.command == "backfill-if-needed":
        service.maybe_run_backfill()
        return 0

    service.run_daily()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
