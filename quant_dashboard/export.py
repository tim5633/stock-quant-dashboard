from __future__ import annotations

import json
from pathlib import Path
from datetime import date, datetime

from sqlalchemy.orm import Session

from quant_dashboard.db import get_latest_metrics, get_recent_runs


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unsupported value type: {type(value)!r}")


def export_dashboard_json(
    session: Session,
    output_path: str,
    run_id: str,
    details: dict,
    timezone: str,
) -> None:
    payload = {
        "run_id": run_id,
        "timezone": timezone,
        "details": details,
        "latest_metrics": get_latest_metrics(session),
        "recent_runs": get_recent_runs(session, limit=20),
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
