"""
Diagnostic — muestra cuantas velas pasan cada filtro del scanner
Ayuda a identificar porque no se generan senales.
"""
import pandas as pd
import numpy as np
from agg_trades_to_db import AggTradeDB

SYMBOL = 'BTCUSDT'
RAW_PATH = f'data/{SYMBOL}/tradebook/raw_data.db'
ANALYTICS_PATH = f'data/{SYMBOL}/tradebook/analytics.db'

INTERVAL = '15 minutes'
START = '2026-02-01'
END = '2026-02-28'

db = AggTradeDB(RAW_PATH, ANALYTICS_PATH, read_only=True)

# OHLC
df = db.get_ohlc(INTERVAL, START, END)
print(f"Velas OHLC: {len(df)}")

# Indicadores
df['volume_ma'] = df['volume'].rolling(window=20).mean()
df['volume_high'] = df['volume'] >= df['volume_ma'] * 1.8
df['delta_normalized'] = df['buy_volume'] / (df['volume'] + 1e-9)

# Volume profile → POC
try:
    df_vol = db.get_volume_profile(INTERVAL, START, END, resolution=10)
    vp_summary = df_vol[df_vol['node_type'] == 'POC'][['open_time', 'price_bin']].copy()
    vp_summary.rename(columns={'price_bin': 'poc'}, inplace=True)
    df = df.merge(vp_summary, on='open_time', how='left')
    print(f"[OK] Volume profile OK — {len(df_vol)} rows, {df['poc'].notna().sum()} con POC")
except Exception as e:
    print(f"[ERROR] Volume profile: {e}")
    df['poc'] = np.nan

# Market context 4H
try:
    df_ctx = db.con.execute(f"""
        SELECT * FROM analytics.market_context_4_hours
        WHERE open_time >= '{START}' AND open_time <= '{END}'
        ORDER BY open_time
    """).fetchdf()
    print(f"[OK] Context 4H: {len(df_ctx)} rows, de {df_ctx['open_time'].min()} a {df_ctx['open_time'].max()}")

    df = df.merge(
        df_ctx[['open_time', 'trend_direction', 'regime', 'last_swing_high', 'last_swing_low']],
        on='open_time', how='left'
    )
except Exception as e:
    print(f"[WARN] Context 4H: {e}")

db.close_connection()

# ---- FILTRADO PROGRESIVO (Absorcion Long) ----
print(f"\n{'='*60}")
print("DIAGNOSTICO — Absorcion Long filter chain")
print(f"{'='*60}")

total = len(df)
print(f"  Velas totales:                    {total}")

f1 = df[df['color'] == 'red']
print(f"  Velas rojas:                      {len(f1)} ({len(f1)/total:.1%})")

f2 = f1[f1.get('volume_high', False)]
print(f"  + volumen > 1.8x MA:               {len(f2)} ({len(f2)/total:.1%})")

f3 = f2[f2['delta_normalized'] < 0.46]
print(f"  + delta_norm < 0.46:              {len(f3)} ({len(f3)/total:.1%})")

# Mecha inferior
def lower_wick_ratio(row):
    total_size = row['high'] - row['low']
    if total_size == 0:
        return 0
    return (min(row['open'], row['close']) - row['low']) / total_size

f4 = f3.copy()
f4['lw_ratio'] = f4.apply(lower_wick_ratio, axis=1)
f4 = f4[f4['lw_ratio'] >= 0.5]
print(f"  + lower wick >= 50%:              {len(f4)} ({len(f4)/total:.1%})")

# POC
f5 = f4.copy()
f5['poc_ok'] = f5['poc'].notna() & (f5['poc'] <= f5['close']) & (f5['poc'] >= f5['low'])
f5 = f5[f5['poc_ok']]
print(f"  + POC between low y close:       {len(f5)} ({len(f5)/total:.1%})")

# Trend
trend_counts = f4['trend_direction'].value_counts(dropna=False)
print(f"\n  Trend direction distribution (del conjunto con mecha+POC):")
for val, cnt in trend_counts.items():
    print(f"    {val!r:20s}: {cnt:>5} ({cnt/len(f4):.1%})")

print(f"\n  Sin filtro de trend: {len(f4)} senales posibles")

# ---- DELTA NORMALIZED STATS ----
print(f"\n{'='*60}")
print("Delta Normalized stats (velas rojas con vol alto):")
dn = f2['delta_normalized']
print(f"  min: {dn.min():.4f}")
print(f"  max: {dn.max():.4f}")
print(f"  median: {dn.median():.4f}")
print(f"  mean: {dn.mean():.4f}")
print(f"  < 0.45: {(dn < 0.45).sum()}")
print(f"  < 0.46: {(dn < 0.46).sum()}")
print(f"  < 0.50: {(dn < 0.50).sum()}")

# Volume stats
print(f"\n{'='*60}")
print("Volume stats (velas rojas):")
vol = f1['volume']
vma = f1['volume_ma']
print(f"  volume_ma mean: {vma.mean():.2f}")
print(f"  volume / volume_ma max: {(vol/vma).max():.2f}")
print(f"  volume >= 1.5x MA: {(vol >= vma * 1.5).sum()}")
print(f"  volume >= 1.8x MA: {(vol >= vma * 1.8).sum()}")
print(f"  volume >= 2.0x MA: {(vol >= vma * 2.0).sum()}")

# POC coverage
print(f"\n{'='*60}")
print(f"POC coverage: {df['poc'].notna().sum()}/{len(df)} ({df['poc'].notna().mean():.1%})")
