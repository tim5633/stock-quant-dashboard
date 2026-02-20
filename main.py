from __future__ import annotations

import argparse
from dataclasses import replace
import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from quant_dashboard.config import load_config
from quant_dashboard.pipeline import run_pipeline


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stock quant dashboard automation")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once immediately and exit",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML config",
    )
    parser.add_argument(
        "--export-json",
        default=None,
        help="Override dashboard json output path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if args.export_json:
        config = replace(config, dashboard_json_path=args.export_json)
    setup_logging(config.log_level)

    if args.once or not config.schedule_enabled:
        run_pipeline(config)
        return

    scheduler = BlockingScheduler(timezone=ZoneInfo(config.timezone))
    trigger = CronTrigger.from_crontab(config.cron, timezone=ZoneInfo(config.timezone))
    scheduler.add_job(run_pipeline, trigger=trigger, args=[config], max_instances=1)
    logging.getLogger(__name__).info(
        "Scheduler started timezone=%s cron=%s", config.timezone, config.cron
    )
    scheduler.start()


if __name__ == "__main__":
    main()
