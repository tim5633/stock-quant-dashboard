from __future__ import annotations

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_dashboard.db import price_data, quant_metrics, utcnow


def _upsert_rows(session: Session, table, rows: list[dict], engine: Engine) -> int:
    if not rows:
        return 0

    now = utcnow()
    enriched_rows = [{**row, "updated_at": now} for row in rows]
    dialect = engine.dialect.name
    pk_cols = [col.name for col in table.primary_key.columns]
    update_cols = [c.name for c in table.columns if c.name not in pk_cols]

    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(table).values(enriched_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=pk_cols,
            set_={col: getattr(stmt.excluded, col) for col in update_cols},
        )
        session.execute(stmt)
    elif dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(table).values(enriched_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=pk_cols,
            set_={col: getattr(stmt.excluded, col) for col in update_cols},
        )
        session.execute(stmt)
    else:
        # Fallback for other dialects: delete by PK then insert.
        for row in enriched_rows:
            where_clause = [table.c[c] == row[c] for c in pk_cols]
            session.execute(table.delete().where(*where_clause))
        session.execute(table.insert(), enriched_rows)

    return len(enriched_rows)


def persist_price_data(session: Session, rows: list[dict], engine: Engine) -> int:
    return _upsert_rows(session=session, table=price_data, rows=rows, engine=engine)


def persist_quant_metrics(session: Session, rows: list[dict], engine: Engine) -> int:
    return _upsert_rows(session=session, table=quant_metrics, rows=rows, engine=engine)
