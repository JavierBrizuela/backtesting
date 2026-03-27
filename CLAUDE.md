# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Backtesting system for cryptocurrency trading analysis using Binance agg_trade data for BTCUSDT. Downloads aggregated trade data, stores it in DuckDB, and creates derived tables for multi-timeframe analysis with order flow metrics.

---

## Environment

- **Python environment**: Pipenv (`pipenv run python script.py`)
- **Database**: DuckDB local file at `data/BTCUSDT/tradebook/agg_trades.db`
- **Windows encoding**: All `print()` with unicode symbols (✓ ✗ ⚠️) must use ASCII alternatives like `[OK]`, `[ERROR]`, `[WARN]` to avoid `cp1252` errors

---

## Commands

```bash
pipenv shell                                # Activate virtual environment
pipenv run python populate_DB.py           # Download data and populate all tables
pipenv run python graph_candlestick.py     # Generate Bokeh HTML chart → bokeh_output/volume_profile.html
```

---

## Architecture

The pipeline has 3 stages:

### 1. Data Ingestion
`download_agg_trades_binance.py` → `populate_DB.py`
- Fetches monthly/daily agg_trades ZIPs from `data.binance.vision`
- Stores raw trades in `agg_trades` table

### 2. DB Class (`agg_trades_to_db.py` — `AggTradeDB`)
All table creation and querying lives here:
- `ohlc_1_minutes` is the base table; higher timeframes computed on-the-fly via `get_ohlc(interval)`
- `create_ohlc_table_from_aggtrades('1 minutes')` — builds OHLC + VWAP + POC from raw trades
- `create_volume_profile_table(resolution, '1 minutes')` — builds per-price-bin volume profile
- `create_market_context_table('15 minutes')` — computes regime metrics over a 20-candle window

### 3. Visualization
`graph_candlestick.py` / `candlestick_analytics.py`
- Reads from DB via `AggTradeDB` methods
- Computes additional pandas columns: `volume_ma`, `delta_cum`, absorption signals
- Detects candle patterns (hammer, etc.)
- Renders with Bokeh to `bokeh_output/volume_profile.html`

---

## Database Schema

### `agg_trades`
Raw tick data from Binance.
Fields: `agg_trade_id`, `price`, `quantity`, `first_trade_id`, `last_trade_id`, `timestamp` (microseconds), `is_buyer_maker`
- `is_buyer_maker = true` → seller taker (aggressive sell)
- `is_buyer_maker = false` → buyer taker (aggressive buy)

### `ohlc_{interval}`
OHLC candlestick data with order flow metrics:
- Standard: `open`, `high`, `low`, `close`, `volume`, `trade_count`
- Order flow: `delta`, `buy_volume`, `sell_volume`
- VWAP metrics: `vwap`, `vwap_std`, `price_vwap_diff`, `vwap_slope`
- `poc` (Point of Control — price level with highest volume)

### `volume_profile`
Volume distribution across price bins:
- `price_bin`, `volume`, `buy_volume`, `sell_volume`, `delta`

### `market_context_{interval}`
Derived from `ohlc_1_minutes` for higher timeframes. Regime detection metrics:
- `efficiency`, `r_squared`, `atr_normalized`, `delta_efficiency`, `coeff_of_variation`

---

## Core Metrics

| Metric | Description |
|---|---|
| OHLC | Standard candlestick + delta, trade_count, buy/sell volume |
| VWAP | Volume Weighted Average Price + std deviation, slope, price diff |
| POC | Point of Control (price level with highest volume per interval) |
| Volume Profile | Volume distribution across price bins |
| Market Context | Efficiency ratio, R², coefficient of variation, ATR normalized, delta efficiency |

---

## Key SQL Notes

- Timestamps in `agg_trades` are in **microseconds** — convert with `to_timestamp(timestamp / 1000000)`
- Uses DuckDB `time_bucket()` for resampling
- `poc_calc` CTE: inner subquery must alias `FLOOR(price)` as `price_bin`, then select `price_bin AS poc` in the outer query — using `price` as alias causes ambiguity
- When joining multiple CTEs, always qualify `open_time` with table alias (e.g. `b.open_time`) to avoid "Ambiguous reference" errors
- `create_market_context_table` has a broken response check (`if response[0] == 'SUCCESS'`) — DuckDB INSERT returns `None`, not a result tuple

---

## Current State (last session)

- VWAP, POC, `vwap_std`, `price_vwap_diff`, `vwap_slope` columns were added to `ohlc_1_minutes` schema
- `create_ohlc_table_from_aggtrades` was refactored to use CTEs: `bucketed → ohlc_base → vwap_calc → poc_calc → with_vwap_slope`
- **Pending fix**: `p.poc` reference in the final SELECT of `create_ohlc_table_from_aggtrades` was mistakenly written as `p.piane` — verify before running
- `populate_DB.py` imports were fixed: `from download_agg_trades_binance import ...` and `from agg_trades_to_db import ...`
- The existing `ohlc_1_minutes` table in DB needs to be **dropped and recreated** to pick up the new schema