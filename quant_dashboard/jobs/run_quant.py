from __future__ import annotations

import pandas as pd


def compute_quant_metrics(price_rows: list[dict]) -> list[dict]:
    if not price_rows:
        return []

    df = pd.DataFrame(price_rows)
    df = df.sort_values(["symbol", "trade_date"]).copy()
    sma_5 = df.groupby("symbol")["close"].transform(
        lambda s: s.rolling(window=5, min_periods=1).mean()
    )
    close_lag_5 = df.groupby("symbol")["close"].shift(5)
    momentum_5d = ((df["close"] / close_lag_5) - 1.0).fillna(0.0)
    signal = (
        ((df["close"] > sma_5) & (momentum_5d > 0))
        .map({True: "BUY", False: "SELL"})
        .astype(str)
    )
    df = df.assign(sma_5=sma_5, close_lag_5=close_lag_5, momentum_5d=momentum_5d, signal=signal)

    result = df[
        ["symbol", "trade_date", "close", "sma_5", "momentum_5d", "signal"]
    ].copy()
    result = result.assign(
        sma_5=result["sma_5"].round(4),
        momentum_5d=result["momentum_5d"].round(6),
    )

    return result.to_dict("records")
