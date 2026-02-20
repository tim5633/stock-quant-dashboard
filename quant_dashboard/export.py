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


def _pct_change(current: float, base: float) -> float:
    if base == 0:
        return 0.0
    return (current / base) - 1.0


def _last_or_none(values: list[float], n: int) -> float | None:
    if len(values) < n:
        return None
    return values[-n]


def _rolling_mean(values: list[float], window: int) -> float:
    if not values:
        return 0.0
    use = values[-min(window, len(values)) :]
    return sum(use) / len(use)


def _std(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


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
        closes = [float(r["close"]) for r in history]
        latest_close = float(latest["close"])
        source = source_by_symbol.get(symbol, "-")

        prev_1 = _last_or_none(closes, 2)
        prev_5 = _last_or_none(closes, 6)
        prev_20 = _last_or_none(closes, 21)
        prev_60 = _last_or_none(closes, 61)

        sma_5 = _rolling_mean(closes, 5)
        sma_20 = _rolling_mean(closes, 20)
        sma_60 = _rolling_mean(closes, 60)
        sma_200 = _rolling_mean(closes, 200)
        high_52w = max(closes[-min(len(closes), 252) :]) if closes else latest_close

        mom_1d = _pct_change(latest_close, prev_1) if prev_1 else 0.0
        mom_5d = _pct_change(latest_close, prev_5) if prev_5 else float(latest["momentum_5d"])
        mom_20d = _pct_change(latest_close, prev_20) if prev_20 else 0.0
        mom_60d = _pct_change(latest_close, prev_60) if prev_60 else 0.0
        returns_10 = [
            _pct_change(closes[i], closes[i - 1]) for i in range(max(1, len(closes) - 9), len(closes))
        ]
        vol_10d = _std(returns_10)
        dist_52w_high = _pct_change(latest_close, high_52w)

        short_score = _clamp_score(50 + mom_5d * 1200 + mom_1d * 600 - vol_10d * 1500)
        short_term.append(
            {
                "symbol": symbol,
                "trade_date": latest["trade_date"],
                "close": latest_close,
                "sma_5": round(sma_5, 4),
                "mom_1d": round(mom_1d, 6),
                "mom_5d": round(mom_5d, 6),
                "vol_10d": round(vol_10d, 6),
                "score": short_score,
                "signal": latest["signal"],
                "source": source,
            }
        )

        mid_window = history[-20:]
        avg_momentum = sum(float(r["momentum_5d"]) for r in mid_window) / len(mid_window)
        mid_is_buy = avg_momentum > 0 and latest_close > sma_20
        mid_score = _clamp_score(
            50
            + mom_20d * 450
            + mom_5d * 220
            + (10 if sma_20 > sma_60 else -10)
        )
        mid_term.append(
            {
                "symbol": symbol,
                "trade_date": latest["trade_date"],
                "close": latest_close,
                "sma_20": round(sma_20, 4),
                "sma_60": round(sma_60, 4),
                "mom_20d": round(mom_20d, 6),
                "mom_5d": round(mom_5d, 6),
                "score": mid_score,
                "signal": "BUY" if mid_is_buy else "SELL",
                "source": source,
            }
        )

        long_is_buy = latest_close > sma_60 and mom_60d > 0
        long_score = _clamp_score(
            50
            + mom_60d * 500
            + (8 if latest_close > sma_60 else -8)
            + (6 if latest_close > sma_200 else -6)
            + dist_52w_high * 160
        )
        long_term.append(
            {
                "symbol": symbol,
                "trade_date": latest["trade_date"],
                "close": latest_close,
                "sma_60": round(sma_60, 4),
                "sma_200": round(sma_200, 4),
                "mom_60d": round(mom_60d, 6),
                "dist_52w_high": round(dist_52w_high, 6),
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
