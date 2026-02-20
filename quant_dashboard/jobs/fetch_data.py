from __future__ import annotations

from datetime import date, timedelta
from io import StringIO
import logging
from typing import Any

import pandas as pd
import requests
import yfinance as yf


logger = logging.getLogger(__name__)


def _fetch_yahoo(symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
    df = yf.download(
        symbol,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    return df


def _fetch_stooq(symbol: str) -> pd.DataFrame:
    symbol_stooq = f"{symbol.lower()}.us"
    url = f"https://stooq.com/q/d/l/?s={symbol_stooq}&i=d"
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    if "No data" in response.text:
        return pd.DataFrame()
    return pd.read_csv(StringIO(response.text))


def fetch_market_data(symbols: list[str], lookback_days: int) -> list[dict]:
    all_rows: list[dict[str, Any]] = []
    today = date.today()
    start_date = today - timedelta(days=lookback_days + 10)
    end_date = today + timedelta(days=1)

    for symbol in symbols:
        df = pd.DataFrame()
        try:
            df = _fetch_yahoo(symbol, start_date, end_date)
        except Exception:
            logger.exception("Failed to fetch Yahoo data for symbol=%s", symbol)

        if df is None or df.empty:
            logger.warning("No Yahoo data for symbol=%s, fallback to Stooq", symbol)
            try:
                df = _fetch_stooq(symbol)
            except Exception:
                logger.exception("Failed to fetch Stooq data for symbol=%s", symbol)
                continue
            if df is None or df.empty:
                logger.warning("No Stooq data for symbol=%s", symbol)
                continue
            df = df.rename(
                columns={
                    "Date": "trade_date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                }
            )
            if "trade_date" not in df.columns:
                logger.warning("Missing trade_date column for symbol=%s", symbol)
                continue
            df = df.copy()
            df = df.assign(trade_date=pd.to_datetime(df["trade_date"]).dt.date)
            df = df.set_index("trade_date")
        else:
            df = df.rename(
                columns={
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                }
            )

        keep_cols = ["open", "high", "low", "close", "volume"]
        if not all(col in df.columns for col in keep_cols):
            logger.warning("Missing expected OHLCV columns for symbol=%s", symbol)
            continue

        df = df[keep_cols].dropna().tail(max(lookback_days * 2, 30))
        for idx, row in df.iterrows():
            trade_date = idx.date() if hasattr(idx, "date") else idx
            all_rows.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "open": round(float(row["open"]), 2),
                    "high": round(float(row["high"]), 2),
                    "low": round(float(row["low"]), 2),
                    "close": round(float(row["close"]), 2),
                    "volume": int(row["volume"]),
                }
            )

    return all_rows
