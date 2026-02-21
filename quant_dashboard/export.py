from __future__ import annotations

import json
from pathlib import Path
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_dashboard.db import get_recent_runs, quant_metrics


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unsupported value type: {type(value)!r}")


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _clamp_score(value: float) -> int:
    return int(round(_clamp(value, 0, 100)))


def _pct_change(current: float, base: float | None) -> float:
    if not base:
        return 0.0
    if base == 0:
        return 0.0
    return (current / base) - 1.0


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


def _safe_prev(values: list[float], back: int) -> float | None:
    idx = len(values) - back - 1
    if idx < 0:
        return None
    return values[idx]


def _compute_stock_table(
    session: Session,
    source_by_symbol: dict[str, str],
    sector_by_symbol: dict[str, str],
    target_annual_return: float,
) -> list[dict]:
    rows = session.execute(
        select(
            quant_metrics.c.symbol,
            quant_metrics.c.trade_date,
            quant_metrics.c.close,
            quant_metrics.c.sma_5,
            quant_metrics.c.momentum_5d,
        ).order_by(quant_metrics.c.symbol, quant_metrics.c.trade_date)
    ).mappings().all()

    by_symbol: dict[str, list[dict]] = {}
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(dict(row))

    table: list[dict] = []
    for symbol, history in by_symbol.items():
        latest = history[-1]
        closes = [float(r["close"]) for r in history]
        current_price = float(latest["close"])

        sma_5 = _rolling_mean(closes, 5)
        sma_20 = _rolling_mean(closes, 20)
        sma_60 = _rolling_mean(closes, 60)
        sma_200 = _rolling_mean(closes, 200)

        prev_1 = _safe_prev(closes, 1)
        prev_5 = _safe_prev(closes, 5)
        prev_20 = _safe_prev(closes, 20)
        prev_60 = _safe_prev(closes, 60)

        mom_1d = _pct_change(current_price, prev_1)
        mom_5d = _pct_change(current_price, prev_5)
        mom_20d = _pct_change(current_price, prev_20)
        mom_60d = _pct_change(current_price, prev_60)

        high_52w = max(closes[-min(252, len(closes)) :])
        dist_52w_high = _pct_change(current_price, high_52w)

        returns_10 = [_pct_change(closes[i], closes[i - 1]) for i in range(max(1, len(closes) - 9), len(closes))]
        vol_10d = _std(returns_10)

        short_score = _clamp_score(50 + mom_5d * 900 + mom_1d * 500 - vol_10d * 1200)
        mid_score = _clamp_score(50 + mom_20d * 420 + mom_5d * 180 + (10 if sma_20 > sma_60 else -10))
        long_score = _clamp_score(
            50 + mom_60d * 380 + (8 if current_price > sma_60 else -8) + (6 if current_price > sma_200 else -6)
        )
        total_score = _clamp_score(0.25 * short_score + 0.35 * mid_score + 0.40 * long_score)

        short_signal = "BUY" if short_score >= 55 else "SELL"
        mid_signal = "BUY" if mid_score >= 55 else "SELL"
        long_signal = "BUY" if long_score >= 55 else "SELL"

        # Simple expected annual return model based on mixed momentum and trend score.
        expected_annual_return = _clamp(
            0.02 + mom_20d * 1.8 + mom_60d * 1.5 + ((total_score - 50) / 100) * 0.12,
            -0.35,
            0.60,
        )

        risk_buffer = _clamp(0.06 + vol_10d * 2.2, 0.05, 0.18)
        reward_buffer = _clamp(max(target_annual_return, 0.08) * 0.65 + mom_20d * 0.6, 0.08, 0.35)

        stop_loss_price = current_price * (1.0 - risk_buffer)
        take_profit_price = current_price * (1.0 + reward_buffer)
        predicted_sell_price = current_price * (1.0 + _clamp(expected_annual_return * 0.5, 0.05, 0.30))

        table.append(
            {
                "symbol": symbol,
                "sector": sector_by_symbol.get(symbol, "Unknown"),
                "source": source_by_symbol.get(symbol, "-"),
                "trade_date": latest["trade_date"],
                "current_price": round(current_price, 2),
                "predicted_sell_price": round(predicted_sell_price, 2),
                "stop_loss_price": round(stop_loss_price, 2),
                "take_profit_price": round(take_profit_price, 2),
                "expected_annual_return": round(expected_annual_return, 6),
                "short_score": short_score,
                "short_signal": short_signal,
                "sma_5": round(sma_5, 4),
                "mom_1d": round(mom_1d, 6),
                "mom_5d": round(mom_5d, 6),
                "vol_10d": round(vol_10d, 6),
                "mid_score": mid_score,
                "mid_signal": mid_signal,
                "sma_20": round(sma_20, 4),
                "sma_60": round(sma_60, 4),
                "mom_20d": round(mom_20d, 6),
                "long_score": long_score,
                "long_signal": long_signal,
                "sma_200": round(sma_200, 4),
                "mom_60d": round(mom_60d, 6),
                "dist_52w_high": round(dist_52w_high, 6),
                "total_score": total_score,
            }
        )

    return sorted(table, key=lambda x: x["total_score"], reverse=True)


def _build_recommendations(stock_table: list[dict], target_annual_return: float, max_recommendations: int) -> list[dict]:
    candidates = [
        row
        for row in stock_table
        if row["expected_annual_return"] >= target_annual_return
        and row["total_score"] >= 55
        and row["mid_signal"] == "BUY"
        and row["long_signal"] == "BUY"
    ]
    candidates.sort(
        key=lambda x: (x["expected_annual_return"], x["total_score"]), reverse=True
    )
    picks = candidates[: max(1, max_recommendations)]
    return [
        {
            "symbol": row["symbol"],
            "sector": row["sector"],
            "current_price": row["current_price"],
            "expected_annual_return": row["expected_annual_return"],
            "predicted_sell_price": row["predicted_sell_price"],
            "stop_loss_price": row["stop_loss_price"],
            "take_profit_price": row["take_profit_price"],
            "total_score": row["total_score"],
        }
        for row in picks
    ]


def export_dashboard_json(
    session: Session,
    output_path: str,
    run_id: str,
    details: dict,
    timezone: str,
    target_annual_return: float,
    max_recommendations: int,
) -> None:
    details = details if isinstance(details, dict) else {}
    source_by_symbol = details.get("source_by_symbol", {})
    sector_by_symbol = details.get("sector_by_symbol", {})

    stock_table = _compute_stock_table(
        session=session,
        source_by_symbol=source_by_symbol,
        sector_by_symbol=sector_by_symbol,
        target_annual_return=target_annual_return,
    )
    recommendations = _build_recommendations(
        stock_table=stock_table,
        target_annual_return=target_annual_return,
        max_recommendations=max_recommendations,
    )

    sectors = sorted({row["sector"] for row in stock_table if row.get("sector")})

    payload = {
        "run_id": run_id,
        "timezone": timezone,
        "details": details,
        "target_annual_return": target_annual_return,
        "stock_table": stock_table,
        "recommended_portfolio": recommendations,
        "sectors": sectors,
        "recent_runs": get_recent_runs(session, limit=20),
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
