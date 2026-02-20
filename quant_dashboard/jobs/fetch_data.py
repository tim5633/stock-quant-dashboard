from __future__ import annotations

from datetime import date, timedelta
import random


def fetch_market_data(symbols: list[str], lookback_days: int) -> list[dict]:
    """Mock market data generator for MVP.

    Replace this with your real provider API integration.
    """
    all_rows: list[dict] = []
    today = date.today()
    start_date = today - timedelta(days=lookback_days)

    for symbol in symbols:
        price = random.uniform(80, 220)
        current = start_date
        while current <= today:
            if current.weekday() < 5:
                change = random.uniform(-0.03, 0.03)
                open_price = price
                close_price = max(1.0, price * (1 + change))
                high = max(open_price, close_price) * random.uniform(1.0, 1.02)
                low = min(open_price, close_price) * random.uniform(0.98, 1.0)
                volume = random.randint(500_000, 5_000_000)
                all_rows.append(
                    {
                        "symbol": symbol,
                        "trade_date": current,
                        "open": round(open_price, 2),
                        "high": round(high, 2),
                        "low": round(low, 2),
                        "close": round(close_price, 2),
                        "volume": volume,
                    }
                )
                price = close_price
            current += timedelta(days=1)

    return all_rows
