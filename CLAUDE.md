# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Backtesting system for cryptocurrency trading analysis using Binance agg_trade data for BTCUSDT. Downloads aggregated trade data, stores it in DuckDB, and creates derived tables for multi-timeframe analysis with order flow metrics.

---

## Environment

- **Python environment**: Pipenv (`pipenv run python script.py`)
- **Database**: DuckDB local files:
  - Raw data: `data/BTCUSDT/tradebook/raw_data.db`
  - Analytics: `data/BTCUSDT/tradebook/analytics.db`
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
- Downloads to temp folder, extracts CSV, imports directly to DuckDB via SQL
- Avoids memory issues by streaming downloads and using DuckDB's `read_csv_auto()`

### 2. DB Class (`agg_trades_to_db.py` — `AggTradeDB`)
All table creation and querying lives here:

**Database architecture:**
- Connects to `raw_data.db` as primary DB
- Attaches `analytics.db` as secondary DB for processed metrics
- All OHLC, volume profile, and market context tables live in `analytics` schema

**Timezone handling:**
- `agg_trades` timestamps are in UTC (microseconds depuis Binance)
- OHLC y volume_profile se almacenan en hora local (Argentina UTC-3 por defecto)
- `self.tz_offset_hours = 3` en `__init__` controla el offset para alinear buckets a UTC boundaries
- `tz_offset_hours` se usa en `create_market_context_table` para que los buckets de 4H queden alineados a 21:00, 01:00, 05:00 AR en vez de 00:00, 04:00, 08:00 AR

**Key methods:**
- `create_ohlc_table(interval)` — creates OHLC schema (no VWAP columns)
- `create_ohlc_table_from_aggtrades(interval)` — builds OHLC + delta directly from `agg_trades` (no VWAP CTEs)
- `create_volume_profile_table(resolution, interval)` — builds per-price-bin volume profile
- `create_market_context_table(interval)` — computes regime metrics + market structure (HH/HL/LH/LL, BOS, CHoCH) over 20-candle window
- `get_ohlc(interval, start_date, end_date)` — queries resampled OHLC from `ohlc_1_minutes` with delta_cumulative and color
- `get_volume_profile(interval, start_date, end_date, resolution)` — reaggregates volume bins with normalization, VWAP/vwap_std, Value Area (70%), HVN/LVN/POC classification
- `get_profile(start_date, end_date, resolution)` — total volume profile across entire period with normalized volume
- `get_institutional_trades(start_date, end_date, interval)` — filters large trades (>5 trades per bucket)

### 3. Visualization
`graph_candlestick.py` / `candlestick_analytics.py`
- Reads from DB via `AggTradeDB` methods
- Computes additional pandas columns: `volume_ma`, `delta_cum`, `delta_normalized`, `volume_high`
- Detects candle patterns (hammer) via `candlestick_analytics.py`
- Renders multi-panel Bokeh chart to `bokeh_output/volume_profile.html`:
  - Main panel: Candlesticks + POC markers + volume profile heatmap + absorption signals
  - Context panel: Efficiency ratio, R², delta efficiency
  - Volume panel: Stacked buy/sell volume + moving averages

---

## Database Schema

### `agg_trades` (in raw_data.db)
Raw tick data from Binance.
Fields: `agg_trade_id`, `price`, `quantity`, `first_trade_id`, `last_trade_id`, `timestamp` (microseconds), `is_buyer_maker`, `is_best_match`
- `is_buyer_maker = true` → seller taker (aggressive sell)
- `is_buyer_maker = false` → buyer taker (aggressive buy)

### `ohlc_{interval}` (in analytics.db)
OHLC candlestick data with order flow metrics:
- Standard: `open`, `high`, `low`, `close`, `volume`, `trade_count`
- Order flow: `delta`, `buy_volume`, `sell_volume`
- Computed at query time: `delta_cumulative`, `color`

### `volume_profile` (in analytics.db)
Volume distribution across price bins:
- Fields: `open_time`, `price_bin`, `trade_count`, `sell_volume`, `buy_volume`, `total_volume`, `delta`
- Primary key: `(open_time, price_bin)`

### `market_context_{interval}` (in analytics.db)
Derived from `ohlc_1_minutes` for higher timeframes. Regime detection metrics:
- `efficiency`, `efficiency_ratio`, `r_squared`, `coefficient_variation`
- `atr_normalized`, `delta_efficiency`
- `regime` (TEXT, nullable): 'RANGE' | 'TREND' | 'TRANSITION'
- `price_location` (TEXT, nullable): 'VAL' | 'VAH' | 'MID' | 'EXTREME'

---

## Core Metrics

| Metric | Description |
|---|---|
| OHLC | Standard candlestick + delta, trade_count, buy/sell volume |
| VWAP | Volume Weighted Average Price + std deviation — computed per interval in `get_volume_profile()` using price_bin midpoints |
| Value Area | Bins que contienen el 70% del volumen total del periodo (`value_area_low`/`value_area_high`) |
| HVN/LVN | High/Low Volume Nodes — dentro/fuera del Value Area |
| Market Context | Efficiency ratio, R², coefficient of variation, ATR normalized, delta efficiency + market structure |

---

## Key SQL Notes

- Timestamps in `agg_trades` are in **microseconds** — convert with `to_timestamp(timestamp / 1000000)`
- Uses DuckDB `time_bucket()` for resampling
- Incremental updates: `create_ohlc_table_from_aggtrades` and `create_volume_profile_table` only process new data by comparing max timestamps
- When joining multiple CTEs, always qualify `open_time` with table alias (e.g. `o.open_time`) to avoid "Ambiguous reference" errors
- POC is computed in the VA CTE as the bin with highest volume per interval

---

## Current State

- **Last commit**: `5c13c21` — Fix: MemoryError en descarga de Binance resuelto con descarga a disco e importación directa a DuckDB
- **Database paths updated**: Raw data moved to `raw_data.db`, analytics to `analytics.db` (separate files)
- **Incremental updates implemented**: OHLC and volume_profile tables support incremental updates by tracking max timestamps
- **POC computation**: Moved from static column to dynamic computation in `get_ohlc()` query
- **Market context table**: Added with efficiency metrics, R², and delta efficiency over 20-candle rolling window
- **Market Structure Detection**: Implemented HH/HL/LH/LL, BOS, CHoCH, and trend_direction in `market_context_4_hours`
- **Performance optimizations**: Added index on `agg_trades.timestamp`, optimized WHERE clause pre-computation
- **Files modified in current session**: `agg_trades_to_db.py`, `graph_candlestick.py`, `populate_DB.py`, `strategy_scanner.py`

### Market Structure Columns (new in `market_context_{interval}`)

```sql
last_swing_high DOUBLE          -- Precio del último Swing High
last_swing_low DOUBLE           -- Precio del último Swing Low
swing_high_time TIMESTAMP       -- Cuándo ocurrió el último SH
swing_low_time TIMESTAMP        -- Cuándo ocurrió el último SL
market_structure_event TEXT     -- 'HH', 'HL', 'LH', 'LL', 'BOS_UP', 'BOS_DOWN', 'CHoCH_UP', 'CHoCH_DOWN'
trend_direction TEXT            -- 'UPTREND', 'DOWNTREND', 'RANGING'
bars_since_structure INTEGER    -- Velas desde último evento
```

**Swing Detection**: 5 velas a cada lado (estándar ICT)
**Source data**: `ohlc_1_minutes` → time_bucket a 4H (u otro intervalo)

---

## Implementation Details

### `create_ohlc_table_from_aggtrades` CTE flow:
1. Direct aggregation from `agg_trades` with `time_bucket`
2. Computes OHLC via `FIRST()` with ORDER BY timestamp
3. Sums `sell_volume`, `buy_volume`, `volume`, `delta`, `trade_count`
4. No VWAP computation — VWAP removed from OHLC tables

### `get_ohlc()` simplified flow:
1. `ohlc_resampled` — reaggregates 1m OHLC to target interval via `time_bucket`
2. Final SELECT adds `delta_cumulative` (running sum) and `color`
3. No POC, no VWAP — those are computed separately if needed

### Market Structure Detection (in `create_market_context_table`):
**CTEs:**
1. `swing_highs` — detecta máximos locales (5 velas a cada lado)
2. `swing_lows` — detecta mínimos locales (5 velas a cada lado)
3. `swings_combined` — forward-fill de últimos SH/SL
4. `structure_detection` — etiqueta HH/HL/LH/LL, detecta BOS
5. `with_choch` — identifica cambios de carácter (CHoCH)

**Usage in scanner:**
- Absorción Long: solo si `trend_direction = 'UPTREND'` o cerca de `last_swing_low`
- Absorción Short: solo si `trend_direction = 'DOWNTREND'` o cerca de `last_swing_high`

### `get_volume_profile()` CTE flow:
1. `resampled` — reaggregates 1m volume_profile bins to target interval
2. `normalized` — local/global volume and delta normalization
3. `vwap_calc` — VWAP and VWAP std dev per interval using (price_bin + half_res) * volume
4. `running` — cumulative volume ordered by volume DESC for Value Area calculation
5. `va` — extracts POC, VAL (value_area_low), VAH (value_area_high) from 70% cumulative volume
6. Final JOIN adds vwap, vwap_std, value bounds, and classifies nodes: POC / HVN / LVN

### Value Area calculation:
- Bins sorted by total_volume DESC within each interval
- Accumulate volume until reaching 70% of total volume
- `val` = lowest price bin in the 70% area
- `vah` = highest price bin in the 70% area
- `POC` = single bin with highest volume
- `HVN` = bins BETWEEN val AND vah (accepted price zones)
- `LVN` = bins outside the value area (rejected price zones)

### Volume normalization in `get_profile()`:
- `total_volume_normalized` — total_volume / max_total_volume (global)

---

## Performance Optimizations

### Database Indexing
- **Index on `agg_trades.timestamp`**: Creado automáticamente en `import_csv_to_db()` y `save_df_to_db()`
  - Mejora `SELECT MAX(timestamp)` de minutos a <100ms
  - Optimiza filtros de fecha en incremental updates

### Incremental Update Optimization
**Antes (lento):**
```sql
WHERE timestamp > (SELECT MAX(timestamp) FROM analytics.ohlc)  -- Subquery por fila
```

**Ahora (rápido):**
```python
max_timestamp = con.execute("SELECT MAX(timestamp)...").fetchone()[0]  # Una vez
WHERE timestamp > {max_timestamp}  -- Valor literal
```

Aplicado en:
- `create_ohlc_table_from_aggtrades()`
- `create_volume_profile_table()`
- `create_market_context_table()`
