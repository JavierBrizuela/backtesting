"""
Estrategias implementadas usando el framework base_strategy.

Estrategias:
  1. Absorcion Long — vendedor agresivo pero precio no baja (rejection en uptrend)
  2. Absorcion Short — comprador agresivo pero precio no sube (rejection en downtrend)
  3. MeanReversionVA — rebote de VAL/VAH hacia POC (filtrado por rango)
  4. DeltaDivergence — precio hace nuevo high/low pero delta no confirma
"""

from base_strategy import Strategy, Signal
import pandas as pd
import numpy as np


# ============================================================
# Helpers compartidos
# ============================================================

def candle_proportions(row):
    body = abs(row['close'] - row['open'])
    total = row['high'] - row['low']
    if total == 0:
        return 0, 0, 0
    body_ratio = body / total
    upper_wick = (row['high'] - max(row['open'], row['close'])) / total
    lower_wick = (min(row['open'], row['close']) - row['low']) / total
    return body_ratio, upper_wick, lower_wick


def check_trend_filter(row, direction):
    """
    Filtro de market structure.
    LONG:  UPTREND o RANGING
    SHORT: DOWNTREND o RANGING

    En RANGING no se requiere proximidad a swing levels porque
    es el regime mas comun en mercados crypto intraday.
    """
    trend = row.get('trend_direction')
    if pd.isna(trend):
        return False

    if direction == 'LONG':
        return trend in ('UPTREND', 'RANGING')
    else:
        return trend in ('DOWNTREND', 'RANGING')


# ============================================================
# 1. Absorcion Long
# ============================================================

class AbsorcionLong(Strategy):
    """
    Vela roja con volumen alto + delta bajo + mecha inferior larga + POC cercano.
    Solo en UPTREND o cerca de swing low.
    """
    name = "Absorcion Long"
    params = {
        "rr_ratio": 2.0,
        "max_candles": 20,
        "volume_mult": 1.8,
        "delta_thresh": 0.46,
        "min_wick_ratio": 0.5,
    }

    def scan(self, df: pd.DataFrame, **kwargs):
        p = self.params
        signals = []

        for _, row in df.iterrows():
            if row['color'] != 'red':
                continue
            if not row.get('volume_high', False):
                continue
            if row['delta_normalized'] >= p['delta_thresh']:
                continue

            body_ratio, upper_wick, lower_wick = candle_proportions(row)
            if lower_wick < p['min_wick_ratio']:
                continue

            # POC por debajo del close (precio cayo HACIA el POC)
            poc = row.get('poc')
            if pd.isna(poc) or poc > row['close'] or poc < row['low']:
                continue

            if not check_trend_filter(row, 'LONG'):
                continue

            risk = row['high'] - row['low']
            signals.append(Signal(
                timestamp=row['open_time'],
                strategy=self.name,
                direction='LONG',
                entry_trigger=row['high'],
                stop_loss=row['low'],
                target=row['high'] + risk * p['rr_ratio'],
                price_open=row['open'],
                price_high=row['high'],
                price_low=row['low'],
                price_close=row['close'],
                metadata={
                    'poc': poc,
                    'delta': row.get('delta'),
                    'delta_normalized': row['delta_normalized'],
                    'lower_wick_ratio': lower_wick,
                    'trend_direction': row.get('trend_direction'),
                },
            ))
        return signals


# ============================================================
# 2. Absorcion Short
# ============================================================

class AbsorcionShort(Strategy):
    """
    Vela verde con volumen alto + delta alto + mecha superior larga + POC cercano.
    Solo en DOWNTREND o cerca de swing high.
    """
    name = "Absorcion Short"
    params = {
        "rr_ratio": 2.0,
        "max_candles": 20,
        "volume_mult": 1.8,
        "delta_thresh": 0.54,
        "min_wick_ratio": 0.5,
    }

    def scan(self, df: pd.DataFrame, **kwargs):
        p = self.params
        signals = []

        for _, row in df.iterrows():
            if row['color'] != 'green':
                continue
            if not row.get('volume_high', False):
                continue
            if row['delta_normalized'] <= p['delta_thresh']:
                continue

            body_ratio, upper_wick, lower_wick = candle_proportions(row)
            if upper_wick < p['min_wick_ratio']:
                continue

            poc = row.get('poc')
            if pd.isna(poc) or poc < row['close'] or poc > row['high']:
                continue

            if not check_trend_filter(row, 'SHORT'):
                continue

            risk = row['high'] - row['low']
            signals.append(Signal(
                timestamp=row['open_time'],
                strategy=self.name,
                direction='SHORT',
                entry_trigger=row['low'],
                stop_loss=row['high'],
                target=row['low'] - risk * p['rr_ratio'],
                price_open=row['open'],
                price_high=row['high'],
                price_low=row['low'],
                price_close=row['close'],
                metadata={
                    'poc': poc,
                    'delta': row.get('delta'),
                    'delta_normalized': row['delta_normalized'],
                    'upper_wick_ratio': upper_wick,
                    'trend_direction': row.get('trend_direction'),
                },
            ))
        return signals


# ============================================================
# 3. Mean Reversion en Value Area
# ============================================================

class MeanReversionVA(Strategy):
    """
    Cuando el precio toca VAL o VAH y revierte hacia el POC.

    LONG:  precio cerca o toco VAL, luego vela de rechazo → target POC
    SHORT: precio cerca o toco VAH, luego vela de rechazo → target POC

    Filtro: market_context dice RANGING (no entrar en tendencia fuerte).
    """
    name = "Mean Reversion VA"
    params = {
        "rr_ratio": 2.0,
        "max_candles": 20,
        "val_tolerance": 0.003,    # 0.3% de tolerancia desde VAL
        "vah_tolerance": 0.003,    # 0.3% de tolerancia desde VAH
        "min_wick_ratio": 0.4,
    }

    def scan(self, df: pd.DataFrame, **kwargs):
        """
        df: ohlc con poc, val, vah mergeados (kwargs debe tener df_vol_profile)
        """
        p = self.params
        df_vol = kwargs.get('df_vol_profile')
        if df_vol is None or df_vol.empty:
            return []

        # Agrupar volume profile por open_time para extraer POC/VAL/VAH
        vp_summary = df_vol[df_vol['node_type'].notna()].groupby('open_time').agg(
            poc=('price_bin', 'first'),
            val=('value_area_low', 'first'),
            vah=('value_area_high', 'first'),
        ).reset_index()

        # Merge OHLC con VP summary
        merged = df.merge(vp_summary, on='open_time', how='left')

        signals = []
        for _, row in merged.iterrows():
            # Solo operar en RANGING
            regime = row.get('regime') or row.get('trend_direction')
            if pd.notna(regime) and regime not in ('RANGING', 'RANGE'):
                continue

            val = row.get('val')
            vah = row.get('vah')
            poc = row.get('poc')

            if any(pd.isna([val, vah, poc])):
                continue

            body_ratio, upper_wick, lower_wick = candle_proportions(row)

            # --- LONG: cerca de VAL con mecha inferior de rechazo ---
            near_val = abs(row['low'] - val) / row['close'] <= p['val_tolerance']
            if near_val and lower_wick >= p['min_wick_ratio']:
                entry = row['high']
                stop = row['low']
                risk = entry - stop
                target = poc  # Target es el POC

                if risk <= 0 or target <= entry:
                    continue

                signals.append(Signal(
                    timestamp=row['open_time'],
                    strategy=self.name,
                    direction='LONG',
                    entry_trigger=entry,
                    stop_loss=stop,
                    target=target,
                    price_open=row['open'],
                    price_high=row['high'],
                    price_low=row['low'],
                    price_close=row['close'],
                    metadata={
                        'poc': poc, 'val': val, 'vah': vah,
                        'lower_wick_ratio': lower_wick,
                    },
                ))

            # --- SHORT: cerca de VAH con mecha superior de rechazo ---
            near_vah = abs(row['high'] - vah) / row['close'] <= p['vah_tolerance']
            if near_vah and upper_wick >= p['min_wick_ratio']:
                entry = row['low']
                stop = row['high']
                risk = stop - entry
                target = poc

                if risk <= 0 or target >= entry:
                    continue

                signals.append(Signal(
                    timestamp=row['open_time'],
                    strategy=self.name,
                    direction='SHORT',
                    entry_trigger=entry,
                    stop_loss=stop,
                    target=target,
                    price_open=row['open'],
                    price_high=row['high'],
                    price_low=row['low'],
                    price_close=row['close'],
                    metadata={
                        'poc': poc, 'val': val, 'vah': vah,
                        'upper_wick_ratio': upper_wick,
                    },
                ))

        return signals


# ============================================================
# 4. Delta Divergence
# ============================================================

class DeltaDivergence(Strategy):
    """
    Detecta divergencias entre precio y delta cumulativo.

    SHORT (Bearish Divergence):
      Precio hace higher high, pero delta_cumulative hace lower high
      → Los buyers pierden fuerza aunque el precio sube

    LONG (Bullish Divergence):
      Precio hace lower low, pero delta_cumulative hace higher low
      → Los sellers pierden fuerza aunque el precio baja

    Entrada: tras confirmar la divergencia con una vela de confirmacion
    """
    name = "Delta Divergence"
    params = {
        "rr_ratio": 2.0,
        "max_candles": 20,
        "lookback": 20,      # Ventana para detectar pivots de precio y delta
        "delta_min_change": 5.0,  # Cambio minimo en delta para considerar divergencia relevante
        "min_distance_candles": 5, # Minimo de velas entre los dos pivots
    }

    def scan(self, df: pd.DataFrame, **kwargs):
        p = self.params
        lookback = p['lookback']
        signals = []

        # Necesitamos delta_cumulative
        if 'delta_cumulative' not in df.columns:
            df = df.copy()
            df['delta_cumulative'] = df['delta'].cumsum()

        for i in range(lookback + 2, len(df)):
            window = df.iloc[i - lookback:i]
            close = window['close'].values
            delta = window['delta_cumulative'].values
            n = len(window)

            # Detectar pivot de precio: peak o valley
            # Buscar maximo de precio en la ventana
            price_high_idx = np.argmax(close)
            price_low_idx = np.argmin(close)
            delta_high_idx = np.argmax(delta)
            delta_low_idx = np.argmin(delta)

            # --- BEARISH DIVERGENCE (SHORT) ---
            # Precio hace HH en position i-1 o i-2, pero delta hace LH
            if price_high_idx >= p['min_distance_candles'] and price_high_idx < n - 2:
                prev_high_idx = np.argmax(close[:price_high_idx]) if price_high_idx > 0 else -1
                if prev_high_idx >= 0:
                    # Precio: el ultimo high es mayor que el anterior
                    if close[price_high_idx] > close[prev_high_idx]:
                        delta_at_prev = delta[prev_high_idx]
                        # Buscar delta en el momento del price prev high
                        # Buscamos el delta peak mas cercano al prev_high_idx
                        delta_lookback = delta[prev_high_idx - 3:prev_high_idx + 3] if prev_high_idx >= 3 else delta[:prev_high_idx + 3]
                        delta_at_prev_peak = np.max(delta_lookback) if len(delta_lookback) > 0 else delta[prev_high_idx]

                        delta_at_curr = delta[price_high_idx]
                        # Delta peak actual es menor que el anterior = divergencia
                        if delta_at_curr < delta_at_prev_peak and (delta_at_prev_peak - delta_at_curr) > p['delta_min_change']:
                            row = df.iloc[i - 1]
                            entry = row['low']
                            stop = row['high']
                            risk = stop - entry
                            if risk > 0:
                                signals.append(Signal(
                                    timestamp=row['open_time'],
                                    strategy=self.name,
                                    direction='SHORT',
                                    entry_trigger=entry,
                                    stop_loss=stop,
                                    target=entry - risk * p['rr_ratio'],
                                    price_open=row['open'],
                                    price_high=row['high'],
                                    price_low=row['low'],
                                    price_close=row['close'],
                                    metadata={
                                        'divergence_type': 'BEARISH',
                                        'price_peak': close[price_high_idx],
                                        'delta_peak': delta_at_curr,
                                    },
                                ))

            # --- BULLISH DIVERGENCE (LONG) ---
            if price_low_idx >= p['min_distance_candles'] and price_low_idx < n - 2:
                prev_low_idx = np.argmin(close[:price_low_idx]) if price_low_idx > 0 else -1
                if prev_low_idx >= 0:
                    if close[price_low_idx] < close[prev_low_idx]:
                        delta_at_prev = delta[prev_low_idx]
                        delta_lookback = delta[prev_low_idx - 3:prev_low_idx + 3] if prev_low_idx >= 3 else delta[:prev_low_idx + 3]
                        delta_at_prev_valley = np.min(delta_lookback) if len(delta_lookback) > 0 else delta[prev_low_idx]

                        delta_at_curr = delta[price_low_idx]
                        if delta_at_curr > delta_at_prev_valley and (delta_at_curr - delta_at_prev_valley) > p['delta_min_change']:
                            row = df.iloc[i - 1]
                            entry = row['high']
                            stop = row['low']
                            risk = entry - stop
                            if risk > 0:
                                signals.append(Signal(
                                    timestamp=row['open_time'],
                                    strategy=self.name,
                                    direction='LONG',
                                    entry_trigger=entry,
                                    stop_loss=stop,
                                    target=entry + risk * p['rr_ratio'],
                                    price_open=row['open'],
                                    price_high=row['high'],
                                    price_low=row['low'],
                                    price_close=row['close'],
                                    metadata={
                                        'divergence_type': 'BULLISH',
                                        'price_trough': close[price_low_idx],
                                        'delta_trough': delta_at_curr,
                                    },
                                ))

        return signals
