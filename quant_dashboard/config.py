from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    app_name: str
    timezone: str
    symbols: list[str]
    lookback_days: int
    snapshot_retention_days: int
    schedule_enabled: bool
    cron: str
    dashboard_json_path: str
    database_url: str
    log_level: str


def load_config(config_path: str = "config.yaml") -> AppConfig:
    load_dotenv()
    raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))

    app = raw.get("app", {})
    pipeline = raw.get("pipeline", {})
    schedule = raw.get("schedule", {})
    output = raw.get("output", {})

    return AppConfig(
        app_name=app.get("name", "stock-quant-dashboard"),
        timezone=app.get("timezone", "UTC"),
        symbols=pipeline.get("symbols", ["AAPL"]),
        lookback_days=int(pipeline.get("lookback_days", 40)),
        snapshot_retention_days=int(pipeline.get("snapshot_retention_days", 14)),
        schedule_enabled=bool(schedule.get("enabled", True)),
        cron=schedule.get("cron", "0 18 * * 1-5"),
        dashboard_json_path=output.get("dashboard_json_path", "docs/data/latest.json"),
        database_url=os.getenv("DATABASE_URL", "sqlite:///./data/quant.db"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
