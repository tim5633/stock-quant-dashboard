from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    select,
    update,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


metadata = MetaData()

price_data = Table(
    "price_data",
    metadata,
    Column("symbol", String(16), primary_key=True),
    Column("trade_date", Date, primary_key=True),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", Integer, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

quant_metrics = Table(
    "quant_metrics",
    metadata,
    Column("symbol", String(16), primary_key=True),
    Column("trade_date", Date, primary_key=True),
    Column("close", Float, nullable=False),
    Column("sma_5", Float, nullable=False),
    Column("momentum_5d", Float, nullable=False),
    Column("signal", String(32), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

dashboard_snapshot = Table(
    "dashboard_snapshot",
    metadata,
    Column("snapshot_id", String(64), primary_key=True),
    Column("symbol", String(16), primary_key=True),
    Column("trade_date", Date, nullable=False),
    Column("close", Float, nullable=False),
    Column("sma_5", Float, nullable=False),
    Column("momentum_5d", Float, nullable=False),
    Column("signal", String(32), nullable=False),
    Column("generated_at", DateTime(timezone=True), nullable=False),
)

pipeline_runs = Table(
    "pipeline_runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True), nullable=True),
    Column("status", String(16), nullable=False),
    Column("rows_written", Integer, nullable=False, default=0),
    Column("details", JSON, nullable=True),
    Column("error_message", String(500), nullable=True),
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def create_db_engine(database_url: str) -> Engine:
    return create_engine(database_url, future=True)


def init_db(engine: Engine) -> None:
    metadata.create_all(engine)


def new_run_id() -> str:
    return uuid4().hex


def record_run_started(session: Session, run_id: str) -> None:
    session.execute(
        pipeline_runs.insert().values(
            run_id=run_id,
            started_at=utcnow(),
            status="RUNNING",
            rows_written=0,
        )
    )


def record_run_finished(
    session: Session,
    run_id: str,
    status: str,
    rows_written: int,
    details: dict | None = None,
    error_message: str | None = None,
) -> None:
    session.execute(
        update(pipeline_runs)
        .where(pipeline_runs.c.run_id == run_id)
        .values(
            finished_at=utcnow(),
            status=status,
            rows_written=rows_written,
            details=details,
            error_message=error_message,
        )
    )


def delete_old_snapshots(session: Session, keep_after: datetime) -> int:
    result = session.execute(
        delete(dashboard_snapshot).where(dashboard_snapshot.c.generated_at < keep_after)
    )
    return result.rowcount or 0


def get_latest_metrics(session: Session) -> list[dict]:
    stmt = (
        select(
            quant_metrics.c.symbol,
            quant_metrics.c.trade_date,
            quant_metrics.c.close,
            quant_metrics.c.sma_5,
            quant_metrics.c.momentum_5d,
            quant_metrics.c.signal,
        )
        .order_by(quant_metrics.c.symbol, quant_metrics.c.trade_date.desc())
    )
    rows = session.execute(stmt).mappings().all()
    latest_by_symbol: dict[str, dict] = {}
    for row in rows:
        symbol = row["symbol"]
        if symbol not in latest_by_symbol:
            latest_by_symbol[symbol] = dict(row)
    return list(latest_by_symbol.values())


def get_recent_runs(session: Session, limit: int = 20) -> list[dict]:
    stmt = (
        select(
            pipeline_runs.c.run_id,
            pipeline_runs.c.started_at,
            pipeline_runs.c.finished_at,
            pipeline_runs.c.status,
            pipeline_runs.c.rows_written,
            pipeline_runs.c.error_message,
        )
        .order_by(pipeline_runs.c.started_at.desc())
        .limit(limit)
    )
    return [dict(row) for row in session.execute(stmt).mappings().all()]
