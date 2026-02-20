from __future__ import annotations

import json
from pathlib import Path
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_dashboard.db import get_latest_metrics, get_recent_runs, quant_metrics


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unsupported value type: {type(value)!r}")


def _clamp_score(value: float) -> int:
    return max(0, min(100, int(round(value))))


def _build_horizon_tables(
    session: Session, source_by_symbol: dict[str, str] | None = None
) -> dict[str, list[dict]]:
    source_by_symbol = source_by_symbol or {}
    rows = session.execute(
        select(
            quant_metrics.c.symbol,
            quant_metrics.c.trade_date,
            quant_metrics.c.close,
            quant_metrics.c.sma_5,
            quant_metrics.c.momentum_5d,
            quant_metrics.c.signal,
        ).order_by(quant_metrics.c.symbol, quant_metrics.c.trade_date)
    ).mappings().all()

    by_symbol: dict[str, list[dict]] = {}
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(dict(row))

    long_term: list[dict] = []
    mid_term: list[dict] = []
    short_term: list[dict] = []

    for symbol, history in by_symbol.items():
        latest = history[-1]
        source = source_by_symbol.get(symbol, "-")

        short_score = _clamp_score(50 + float(latest["momentum_5d"]) * 1200)
        short_term.append(
            {
                "symbol": symbol,
                "trade_date": latest["trade_date"],
                "close": latest["close"],
                "score": short_score,
                "signal": latest["signal"],
                "source": source,
            }
        )

        mid_window = history[-20:]
        avg_momentum = sum(float(r["momentum_5d"]) for r in mid_window) / len(mid_window)
        mid_is_buy = avg_momentum > 0 and float(latest["close"]) > float(latest["sma_5"])
        mid_score = _clamp_score(
            50
            + avg_momentum * 1000
            + (8 if float(latest["close"]) > float(latest["sma_5"]) else -8)
        )
        mid_term.append(
            {
                "symbol": symbol,
                "trade_date": latest["trade_date"],
                "close": latest["close"],
                "score": mid_score,
                "signal": "BUY" if mid_is_buy else "SELL",
                "source": source,
            }
        )

        long_window = history[-60:]
        baseline = sum(float(r["close"]) for r in long_window) / len(long_window)
        drift = (float(latest["close"]) / float(long_window[0]["close"])) - 1.0
        long_is_buy = float(latest["close"]) > baseline and drift > 0
        long_score = _clamp_score(
            50
            + drift * 300
            + ((float(latest["close"]) / baseline) - 1.0) * 250
        )
        long_term.append(
            {
                "symbol": symbol,
                "trade_date": latest["trade_date"],
                "close": latest["close"],
                "score": long_score,
                "signal": "BUY" if long_is_buy else "SELL",
                "source": source,
            }
        )

    return {
        "long_term": sorted(long_term, key=lambda x: x["score"], reverse=True),
        "mid_term": sorted(mid_term, key=lambda x: x["score"], reverse=True),
        "short_term": sorted(short_term, key=lambda x: x["score"], reverse=True),
    }


def export_dashboard_json(
    session: Session,
    output_path: str,
    run_id: str,
    details: dict,
    timezone: str,
) -> None:
    source_by_symbol = details.get("source_by_symbol", {}) if isinstance(details, dict) else {}
    horizon = _build_horizon_tables(session, source_by_symbol=source_by_symbol)
    payload = {
        "run_id": run_id,
        "timezone": timezone,
        "details": details,
        "latest_metrics": get_latest_metrics(session),
        "long_term": horizon["long_term"],
        "mid_term": horizon["mid_term"],
        "short_term": horizon["short_term"],
        "recent_runs": get_recent_runs(session, limit=20),
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
