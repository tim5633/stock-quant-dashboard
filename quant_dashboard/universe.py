from __future__ import annotations

import logging
from io import StringIO

import pandas as pd
import requests

from quant_dashboard.config import AppConfig


logger = logging.getLogger(__name__)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def _resolve_manual(symbols: list[str], max_symbols: int) -> tuple[list[str], dict[str, str]]:
    unique: list[str] = []
    seen = set()
    for symbol in symbols:
        s = _normalize_symbol(symbol)
        if s and s not in seen:
            seen.add(s)
            unique.append(s)
        if len(unique) >= max_symbols:
            break
    return unique, {s: "Manual" for s in unique}


def _resolve_sp500(max_symbols: int) -> tuple[list[str], dict[str, str]]:
    tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    if not tables:
        raise RuntimeError("Failed to load S&P 500 table")
    table = tables[0]
    symbols: list[str] = []
    sectors: dict[str, str] = {}
    for _, row in table.iterrows():
        symbol = _normalize_symbol(str(row.get("Symbol", "")))
        if not symbol:
            continue
        symbols.append(symbol)
        sectors[symbol] = str(row.get("GICS Sector", "Unknown"))
        if len(symbols) >= max_symbols:
            break
    return symbols, sectors


def _resolve_all_us(max_symbols: int) -> tuple[list[str], dict[str, str]]:
    urls = [
        "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
    ]
    symbols: list[str] = []
    seen = set()
    for url in urls:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text:
            continue
        df = pd.read_csv(StringIO(text), sep="|")
        if "Symbol" in df.columns:
            candidates = df["Symbol"].astype(str).tolist()
        elif "ACT Symbol" in df.columns:
            candidates = df["ACT Symbol"].astype(str).tolist()
        else:
            continue

        for raw in candidates:
            symbol = _normalize_symbol(raw)
            if not symbol or symbol in seen:
                continue
            if "$" in symbol:
                continue
            seen.add(symbol)
            symbols.append(symbol)
            if len(symbols) >= max_symbols:
                break
        if len(symbols) >= max_symbols:
            break

    return symbols, {s: "Unknown" for s in symbols}


def resolve_universe(config: AppConfig) -> tuple[list[str], dict[str, str]]:
    mode = config.universe.lower().strip()
    if mode == "manual":
        return _resolve_manual(config.symbols, config.max_symbols)
    if mode == "sp500":
        return _resolve_sp500(config.max_symbols)
    if mode == "all_us":
        logger.warning("all_us mode is broad; capped at max_symbols=%s", config.max_symbols)
        return _resolve_all_us(config.max_symbols)
    raise ValueError(f"Unsupported universe mode: {config.universe}")
