from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from quant_dashboard.config import AppConfig
from quant_dashboard.db import (
    create_db_engine,
    init_db,
    new_run_id,
    record_run_finished,
    record_run_started,
)
from quant_dashboard.export import export_dashboard_json
from quant_dashboard.jobs.fetch_data import fetch_market_data
from quant_dashboard.jobs.persist import persist_price_data, persist_quant_metrics
from quant_dashboard.jobs.run_quant import compute_quant_metrics
from quant_dashboard.jobs.update_dashboard import refresh_dashboard_snapshot


logger = logging.getLogger(__name__)


def _latest_source_by_symbol(price_rows: list[dict]) -> dict[str, str]:
    latest: dict[str, tuple] = {}
    for row in price_rows:
        symbol = row.get("symbol")
        trade_date = row.get("trade_date")
        source = row.get("source")
        if not symbol or not trade_date or not source:
            continue
        if symbol not in latest or trade_date > latest[symbol][0]:
            latest[symbol] = (trade_date, source)
    return {symbol: value[1] for symbol, value in latest.items()}


def run_pipeline(config: AppConfig) -> str:
    engine = create_db_engine(config.database_url)
    init_db(engine)

    run_id = new_run_id()
    rows_written = 0
    details: dict = {}

    with Session(engine) as session:
        record_run_started(session, run_id)
        session.commit()

        try:
            prices = fetch_market_data(config.symbols, config.lookback_days)
            if not prices:
                raise RuntimeError("No market data fetched from providers")
            metrics = compute_quant_metrics(prices)

            price_rows = persist_price_data(session, prices, engine=engine)
            metric_rows = persist_quant_metrics(session, metrics, engine=engine)
            snapshot_result = refresh_dashboard_snapshot(
                session, run_id=run_id, retention_days=config.snapshot_retention_days
            )

            rows_written = price_rows + metric_rows + snapshot_result["snapshot_rows"]
            details = {
                "price_rows": price_rows,
                "metric_rows": metric_rows,
                "source_by_symbol": _latest_source_by_symbol(prices),
                **snapshot_result,
            }
            record_run_finished(
                session,
                run_id=run_id,
                status="SUCCESS",
                rows_written=rows_written,
                details=details,
            )
            export_dashboard_json(
                session=session,
                output_path=config.dashboard_json_path,
                run_id=run_id,
                details=details,
                timezone=config.timezone,
            )
            session.commit()
            logger.info("run_id=%s status=SUCCESS details=%s", run_id, details)
            return run_id
        except Exception as exc:
            session.rollback()
            record_run_finished(
                session,
                run_id=run_id,
                status="FAILED",
                rows_written=rows_written,
                details=details,
                error_message=str(exc)[:500],
            )
            session.commit()
            logger.exception("run_id=%s status=FAILED", run_id)
            raise
