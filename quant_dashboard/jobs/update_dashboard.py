from __future__ import annotations

from datetime import timedelta

from sqlalchemy.orm import Session

from quant_dashboard.db import (
    dashboard_snapshot,
    delete_old_snapshots,
    get_latest_metrics,
    utcnow,
)


def refresh_dashboard_snapshot(
    session: Session, run_id: str, retention_days: int
) -> dict[str, int]:
    latest_rows = get_latest_metrics(session)
    generated_at = utcnow()
    payload = [
        {
            "snapshot_id": run_id,
            "symbol": row["symbol"],
            "trade_date": row["trade_date"],
            "close": row["close"],
            "sma_5": row["sma_5"],
            "momentum_5d": row["momentum_5d"],
            "signal": row["signal"],
            "generated_at": generated_at,
        }
        for row in latest_rows
    ]

    if payload:
        session.execute(dashboard_snapshot.insert(), payload)

    deleted_count = delete_old_snapshots(
        session=session,
        keep_after=generated_at - timedelta(days=retention_days),
    )
    return {"snapshot_rows": len(payload), "deleted_old_snapshots": deleted_count}
