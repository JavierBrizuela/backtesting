"""
Strategy Scanner - Escanea patrones de Order Flow en la base de datos

Escanea las siguientes estrategias:
1. Absorción Long (en POC/HVN con delta contrario)
2. Absorción Short (en POC/HVN con delta contrario)
3. Imbalance Long (buy volume >> sell volume)
4. Imbalance Short (sell volume >> buy volume)

Genera:
- Tablas CSV con cada señal encontrada
- Backtest de cada señal (TOUCH_TARGET, TOUCH_STOP, MAX_PROFIT, MAX_LOSS)
- HTML filtrado por estrategia para revisión visual
"""

import pandas as pd
import numpy as np
from agg_trades_to_db import AggTradeDB
import os

# ==================== CONFIGURACIÓN ====================

SYMBOL = 'BTCUSDT'
RAW_PATH = f'data/{SYMBOL}/tradebook/raw_data.db'
ANALYTICS_PATH = f'data/{SYMBOL}/tradebook/analytics.db'

# Rango de fechas para escanear
START_DATE = '2026-02-01'
END_DATE = '2026-02-28'
INTERVAL = '15 minutes'

# Thresholds de absorción
ABSORPTION_VOLUME_MA_MULT = 1.8  # Volumen >= 1.8x MA20
ABSORPTION_DELTA_LONG_THRESH = 0.46  # delta_normalized < 0.46 para long
ABSORPTION_DELTA_SHORT_THRESH = 0.54  # delta_normalized > 0.54 para short
ABSORPTION_POC_DISTANCE = 0.2  # POC dentro del 20% del cuerpo
ABSORPTION_MIN_WICK_RATIO = 0.5  # Mecha debe ser >= 50% del tamaño total de la vela

# Thresholds de imbalance
IMBALANCE_RATIO = 3.0
IMBALANCE_MIN_STREAK = 3

# Gestión de riesgo
RR_RATIO = 2.0  # Reward:Risk 2:1
MAX_CANDLES_EXIT = 20  # Salida forzosa después de N velas si no toca target/stop

# Output
OUTPUT_DIR = 'scanner_output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==================== BACKTEST HELPERS ====================

def simulate_trade(signal, df_ohlc, rr_ratio=2.0, max_candles=20):
    """
    Simula una operación desde la señal hasta target/stop/expiración.

    Para LONG:
    - Entry: ruptura del HIGH de la vela señal
    - Stop: LOW de la vela señal
    - Target: entry + (entry - stop) * rr_ratio

    Para SHORT:
    - Entry: ruptura del LOW de la vela señal
    - Stop: HIGH de la vela señal
    - Target: entry - (stop - entry) * rr_ratio

    Returns: dict con resultado de la operación
    """
    signal_time = signal['timestamp']
    signal_idx = df_ohlc[df_ohlc['open_time'] == signal_time].index

    if len(signal_idx) == 0:
        return {
            'exit_time': None,
            'exit_price': None,
            'exit_reason': 'SIGNAL_NOT_FOUND',
            'pnl': 0,
            'max_profit': 0,
            'max_loss': 0,
            'mae': 0,  # Maximum Adverse Excursion
            'mfe': 0,  # Maximum Favorable Excursion
        }

    signal_idx = signal_idx[0]
    future_candles = df_ohlc.iloc[signal_idx + 1:signal_idx + 1 + max_candles].copy()

    if future_candles.empty:
        return {
            'exit_time': None,
            'exit_price': None,
            'exit_reason': 'NO_DATA',
            'pnl': 0,
            'max_profit': 0,
            'max_loss': 0,
            'mae': 0,
            'mfe': 0,
        }

    if signal['type'] in ['ABSORPTION_LONG', 'IMBALANCE_LONG']:
        # LONG trade
        entry_price = signal['entry_trigger']  # HIGH de la vela
        stop_price = signal['stop_loss']  # LOW de la vela
        target_price = entry_price + (entry_price - stop_price) * rr_ratio

        risk = entry_price - stop_price

        # Calcular MFE y MAE
        max_high = future_candles['high'].max()
        min_low = future_candles['low'].min()

        mfe = max_high - entry_price  # Máximo favorables (para long)
        mae = entry_price - min_low  # Máximo adverso (para long)

        # Verificar si tocó target primero
        for idx, candle in future_candles.iterrows():
            if candle['low'] <= stop_price:
                return {
                    'exit_time': candle['open_time'],
                    'exit_price': stop_price,
                    'exit_reason': 'STOP_LOSS',
                    'pnl': -risk,
                    'max_profit': mfe,
                    'max_loss': mae,
                    'mae': mae,
                    'mfe': mfe,
                }
            if candle['high'] >= target_price:
                profit = target_price - entry_price
                return {
                    'exit_time': candle['open_time'],
                    'exit_price': target_price,
                    'exit_reason': 'TARGET',
                    'pnl': profit,
                    'max_profit': mfe,
                    'max_loss': mae,
                    'mae': mae,
                    'mfe': mfe,
                }

        # No tocó ni target ni stop → salida al close de la última vela
        exit_price = future_candles.iloc[-1]['close']
        exit_time = future_candles.iloc[-1]['open_time']
        pnl = exit_price - entry_price

        return {
            'exit_time': exit_time,
            'exit_price': exit_price,
            'exit_reason': 'TIME_EXIT',
            'pnl': pnl,
            'max_profit': mfe,
            'max_loss': mae,
            'mae': mae,
            'mfe': mfe,
        }

    else:  # SHORT
        entry_price = signal['entry_trigger']  # LOW de la vela
        stop_price = signal['stop_loss']  # HIGH de la vela
        target_price = entry_price - (stop_price - entry_price) * rr_ratio

        risk = stop_price - entry_price

        # Calcular MFE y MAE
        max_high = future_candles['high'].max()
        min_low = future_candles['low'].min()

        mfe = entry_price - min_low  # Máximo favorables (para short)
        mae = max_high - entry_price  # Máximo adverso (para short)

        # Verificar si tocó stop primero
        for idx, candle in future_candles.iterrows():
            if candle['high'] >= stop_price:
                return {
                    'exit_time': candle['open_time'],
                    'exit_price': stop_price,
                    'exit_reason': 'STOP_LOSS',
                    'pnl': -risk,
                    'max_profit': mfe,
                    'max_loss': mae,
                    'mae': mae,
                    'mfe': mfe,
                }
            if candle['low'] <= target_price:
                profit = entry_price - target_price
                return {
                    'exit_time': candle['open_time'],
                    'exit_price': target_price,
                    'exit_reason': 'TARGET',
                    'pnl': profit,
                    'max_profit': mfe,
                    'max_loss': mae,
                    'mae': mae,
                    'mfe': mfe,
                }

        # No tocó ni target ni stop → salida al close
        exit_price = future_candles.iloc[-1]['close']
        exit_time = future_candles.iloc[-1]['open_time']
        pnl = entry_price - exit_price

        return {
            'exit_time': exit_time,
            'exit_price': exit_price,
            'exit_reason': 'TIME_EXIT',
            'pnl': pnl,
            'max_profit': mfe,
            'max_loss': mae,
            'mae': mae,
            'mfe': mfe,
        }


def calculate_backtest_metrics(trades_df):
    """Calcula métricas agregadas de backtest."""
    if trades_df.empty:
        return {}

    total_trades = len(trades_df)
    winning_trades = trades_df[trades_df['pnl'] > 0]
    losing_trades = trades_df[trades_df['pnl'] <= 0]

    win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0
    avg_win = winning_trades['pnl'].mean() if len(winning_trades) > 0 else 0
    avg_loss = abs(losing_trades['pnl'].mean()) if len(losing_trades) > 0 else 0

    gross_profit = winning_trades['pnl'].sum() if len(winning_trades) > 0 else 0
    gross_loss = abs(losing_trades['pnl'].sum()) if len(losing_trades) > 0 else 0

    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    # Calcular drawdown máximo
    trades_df = trades_df.copy()
    trades_df['cum_pnl'] = trades_df['pnl'].cumsum()
    running_max = trades_df['cum_pnl'].cummax()
    drawdown = running_max - trades_df['cum_pnl']
    max_drawdown = drawdown.max()

    # MFE y MAE promedio
    avg_mfe = trades_df['mfe'].mean()
    avg_mae = trades_df['mae'].mean()

    # Exit reasons
    exit_reasons = trades_df['exit_reason'].value_counts().to_dict()

    return {
        'total_trades': total_trades,
        'winning_trades': len(winning_trades),
        'losing_trades': len(losing_trades),
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_factor': profit_factor,
        'expectancy': expectancy,
        'max_drawdown': max_drawdown,
        'avg_mfe': avg_mfe,
        'avg_mae': avg_mae,
        'exit_reasons': exit_reasons,
    }


# ==================== DETECCIÓN DE SEÑALES ====================

def calculate_candle_proportions(row):
    """Calcula proporciones de cuerpo y mechas."""
    body = abs(row['close'] - row['open'])
    total = row['high'] - row['low']
    if total == 0:
        return 0, 0, 0
    body_ratio = body / total
    upper_wick = (row['high'] - max(row['open'], row['close'])) / total
    lower_wick = (min(row['open'], row['close']) - row['low']) / total
    return body_ratio, upper_wick, lower_wick


def detect_absorption_long(df):
    """
    Detecta absorción LONG:
    - Vela roja (close < open)
    - Volumen alto (>= 1.8x MA20)
    - Delta bajo (< 0.46) → sellers agresivos pero precio no baja
    - POC por debajo del close (el precio cayó hacia el POC)
    - Mecha inferior larga (>= 50%) → rechazo del nivel
    - Efficiency baja (mercado en rango)
    - Market Structure: UPTREND o en zona de soporte (cerca de last_swing_low)
    """
    signals = []

    for idx, row in df.iterrows():
        # Vela roja (close < open)
        if row['color'] != 'red':
            continue

        # Volumen alto
        if not row.get('volume_high', False):
            continue

        # Delta bajo (sellers pero no baja)
        if row['delta_normalized'] >= ABSORPTION_DELTA_LONG_THRESH:
            continue

        # Calcular proporciones de la vela
        body_ratio, upper_wick, lower_wick = calculate_candle_proportions(row)

        # Mecha inferior larga (>= 50% del total) → RECHAZO
        if lower_wick < ABSORPTION_MIN_WICK_RATIO:
            continue

        # POC por debajo del close (precio cayó HACIA el POC)
        if row['poc'] > row['close'] or row['poc'] < row['low']:
            continue

        # Efficiency baja (opcional, filtro de régimen)
        if 'efficiency_normalized' in row and 'efficiency_ma' in row:
            if pd.notna(row['efficiency_normalized']) and pd.notna(row['efficiency_ma']):
                if row['efficiency_normalized'] >= row['efficiency_ma']:
                    continue

        # FILTRO DE MARKET STRUCTURE: Solo operar Long en UPTREND
        # O cuando estamos cerca del last_swing_low (zona de soporte)
        trend_ok = False
        if 'trend_direction' in row and pd.notna(row['trend_direction']):
            if row['trend_direction'] == 'UPTREND':
                trend_ok = True
            elif row['trend_direction'] == 'RANGING' and 'last_swing_low' in row:
                # En rango, aceptar si estamos cerca del soporte (last_swing_low)
                if pd.notna(row['last_swing_low']):
                    distance_to_sl = abs(row['close'] - row['last_swing_low']) / row['close']
                    if distance_to_sl < 0.01:  # Dentro del 1% del swing low
                        trend_ok = True

        if not trend_ok:
            continue

        signals.append({
            'timestamp': row['open_time'],
            'type': 'ABSORPTION_LONG',
            'price_open': row['open'],
            'price_high': row['high'],
            'price_low': row['low'],
            'price_close': row['close'],
            'poc': row['poc'],
            'volume': row['volume'],
            'delta': row['delta'],
            'delta_normalized': row['delta_normalized'],
            'body_ratio': body_ratio,
            'lower_wick_ratio': lower_wick,
            'trend_direction': row.get('trend_direction', 'UNKNOWN'),
            'market_structure_event': row.get('market_structure_event', 'NONE'),
            'last_swing_low': row.get('last_swing_low', None),
            'entry_trigger': row['high'],
            'stop_loss': row['low'],
            'target': row['high'] + (row['high'] - row['low']) * RR_RATIO,
        })

    return pd.DataFrame(signals)


def detect_absorption_short(df):
    """
    Detecta absorción SHORT:
    - Vela verde (close > open)
    - Volumen alto (>= 1.8x MA20)
    - Delta alto (> 0.54) → buyers agresivos pero precio no sube
    - POC por encima del open (el precio subió HACIA el POC)
    - Mecha superior larga (>= 50%) → rechazo del nivel
    - Efficiency baja (mercado en rango)
    - Market Structure: DOWNTREND o en zona de resistencia (cerca de last_swing_high)
    """
    signals = []

    for idx, row in df.iterrows():
        # Vela verde
        if row['color'] != 'green':
            continue

        # Volumen alto
        if not row.get('volume_high', False):
            continue

        # Delta alto (buyers pero no sube)
        if row['delta_normalized'] <= ABSORPTION_DELTA_SHORT_THRESH:
            continue

        # Calcular proporciones
        body_ratio, upper_wick, lower_wick = calculate_candle_proportions(row)

        # Mecha superior larga (>= 50% del total) → RECHAZO
        if upper_wick < ABSORPTION_MIN_WICK_RATIO:
            continue

        # POC por encima del open (precio subió HACIA el POC)
        if row['poc'] < row['close'] or row['poc'] > row['high']:
            continue

        # Efficiency baja (opcional, filtro de régimen)
        if 'efficiency_normalized' in row and 'efficiency_ma' in row:
            if pd.notna(row['efficiency_normalized']) and pd.notna(row['efficiency_ma']):
                if row['efficiency_normalized'] >= row['efficiency_ma']:
                    continue

        # FILTRO DE MARKET STRUCTURE: Solo operar Short en DOWNTREND
        # O cuando estamos cerca del last_swing_high (zona de resistencia)
        trend_ok = False
        if 'trend_direction' in row and pd.notna(row['trend_direction']):
            if row['trend_direction'] == 'DOWNTREND':
                trend_ok = True
            elif row['trend_direction'] == 'RANGING' and 'last_swing_high' in row:
                # En rango, aceptar si estamos cerca de la resistencia (last_swing_high)
                if pd.notna(row['last_swing_high']):
                    distance_to_sh = abs(row['close'] - row['last_swing_high']) / row['close']
                    if distance_to_sh < 0.01:  # Dentro del 1% del swing high
                        trend_ok = True

        if not trend_ok:
            continue

        signals.append({
            'timestamp': row['open_time'],
            'type': 'ABSORPTION_SHORT',
            'price_open': row['open'],
            'price_high': row['high'],
            'price_low': row['low'],
            'price_close': row['close'],
            'poc': row['poc'],
            'volume': row['volume'],
            'delta': row['delta'],
            'delta_normalized': row['delta_normalized'],
            'body_ratio': body_ratio,
            'upper_wick_ratio': upper_wick,
            'trend_direction': row.get('trend_direction', 'UNKNOWN'),
            'market_structure_event': row.get('market_structure_event', 'NONE'),
            'last_swing_high': row.get('last_swing_high', None),
            'entry_trigger': row['low'],
            'stop_loss': row['high'],
            'target': row['low'] - (row['high'] - row['low']) * RR_RATIO,
        })

    return pd.DataFrame(signals)


def detect_imbalance_signals(df_vol_profile, df_ohlc):
    """
    Detecta desbalances de volumen que probablemente se llenen.
    """
    df = df_vol_profile.sort_values(['open_time', 'price_bin']).reset_index(drop=True)

    # Calcular ratio de imbalance por bin
    df['imbalance_ratio'] = df['buy_volume'] / (df['sell_volume'] + 1e-9)

    # Detectar streaks consecutivos
    def count_streak(group):
        group = group.sort_values('open_time').copy()
        group['streak'] = (group['imbalance_ratio'] >= IMBALANCE_RATIO).astype(int)
        group['streak_count'] = group['streak'].groupby(
            (group['streak'] != group['streak'].shift()).cumsum()
        ).cumsum()
        return group

    df = df.groupby('price_bin', group_keys=False).apply(count_streak)

    # Filtrar bins con streak >= MIN_STREAK y ratio alto
    buy_imbalance = df[
        (df['streak_count'] >= IMBALANCE_MIN_STREAK) &
        (df['imbalance_ratio'] >= IMBALANCE_RATIO)
    ].copy()

    # Para sell imbalance
    df['sell_imbalance_ratio'] = df['sell_volume'] / (df['buy_volume'] + 1e-9)

    def count_sell_streak(group):
        group = group.sort_values('open_time').copy()
        group['sell_streak'] = (group['sell_imbalance_ratio'] >= IMBALANCE_RATIO).astype(int)
        group['sell_streak_count'] = group['sell_streak'].groupby(
            (group['sell_streak'] != group['sell_streak'].shift()).cumsum()
        ).cumsum()
        return group

    df = df.groupby('price_bin', group_keys=False).apply(count_sell_streak)

    sell_imbalance = df[
        (df['sell_streak_count'] >= IMBALANCE_MIN_STREAK) &
        (df['sell_imbalance_ratio'] >= IMBALANCE_RATIO)
    ].copy()

    signals = []

    # Procesar buy imbalances
    for _, row in buy_imbalance.drop_duplicates(subset=['open_time', 'price_bin']).iterrows():
        signals.append({
            'timestamp': row['open_time'],
            'type': 'IMBALANCE_LONG',
            'price_bin': row['price_bin'],
            'imbalance_ratio': row['imbalance_ratio'],
            'buy_volume': row['buy_volume'],
            'sell_volume': row['sell_volume'],
            'streak': row['streak_count'],
            'entry_trigger': row['price_bin'],  # Entry cuando toca el bin
            'stop_loss': row['price_bin'] - (row['price_bin'] * 0.01),  # Stop 1% debajo
            'target': row['price_bin'] + (row['price_bin'] * 0.02),  # Target 2%
        })

    # Procesar sell imbalances
    for _, row in sell_imbalance.drop_duplicates(subset=['open_time', 'price_bin']).iterrows():
        signals.append({
            'timestamp': row['open_time'],
            'type': 'IMBALANCE_SHORT',
            'price_bin': row['price_bin'],
            'imbalance_ratio': row['sell_imbalance_ratio'],
            'buy_volume': row['buy_volume'],
            'sell_volume': row['sell_volume'],
            'streak': row['sell_streak_count'],
            'entry_trigger': row['price_bin'],  # Entry cuando toca el bin
            'stop_loss': row['price_bin'] + (row['price_bin'] * 0.01),  # Stop 1% encima
            'target': row['price_bin'] - (row['price_bin'] * 0.02),  # Target 2%
        })

    return pd.DataFrame(signals)


# ==================== BACKTEST DE SEÑALES ====================

def run_backtest_on_signals(signals_df, df_ohlc):
    """
    Ejecuta backtest en todas las señales detectadas.
    Agrega columnas de resultado a cada señal.
    """
    results = []

    for idx, signal in signals_df.iterrows():
        trade_result = simulate_trade(signal, df_ohlc, rr_ratio=RR_RATIO, max_candles=MAX_CANDLES_EXIT)

        result_row = {
            **signal.to_dict(),
            'exit_time': trade_result['exit_time'],
            'exit_price': trade_result['exit_price'],
            'exit_reason': trade_result['exit_reason'],
            'pnl': trade_result['pnl'],
            'max_profit': trade_result['max_profit'],
            'max_loss': trade_result['max_loss'],
            'mae': trade_result['mae'],
            'mfe': trade_result['mfe'],
        }
        results.append(result_row)

    return pd.DataFrame(results)


# ==================== GENERAR HTML ====================

def generate_summary_html(absorption_long, absorption_short, imbalance):
    """Genera un resumen HTML con todas las señales y resultados de backtest."""

    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Strategy Scanner Summary</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }
            h1 { color: #00ff88; }
            h2 { color: #00ccff; margin-top: 30px; }
            h3 { color: #ffaa00; margin-top: 20px; }
            table { border-collapse: collapse; width: 100%; margin: 20px 0; background: #16213e; }
            th, td { border: 1px solid #333; padding: 8px; text-align: center; }
            th { background: #0f3460; color: #fff; }
            tr:nth-child(even) { background: #1a1a2e; }
            tr:hover { background: #2a2a4e; }
            .long { color: #00ff88; }
            .short { color: #ff4444; }
            .metric { background: #0f3460; padding: 15px; border-radius: 5px; display: inline-block; margin: 10px; }
            .metric-value { font-size: 24px; font-weight: bold; color: #00ff88; }
            .metric-label { font-size: 12px; color: #aaa; }
            .win { color: #00ff88; }
            .loss { color: #ff4444; }
            .neutral { color: #ffaa00; }
        </style>
    </head>
    <body>
        <h1>Strategy Scanner Summary</h1>
        <p>Periodo: """ + START_DATE + """ a """ + END_DATE + """ | Intervalo: """ + INTERVAL + """</p>
        <p>RR Ratio: """ + str(RR_RATIO) + """:1 | Max Candiles: """ + str(MAX_CANDLES_EXIT) + """</p>
    """

    # Métricas generales
    total_long = len(absorption_long) if not absorption_long.empty else 0
    total_short = len(absorption_short) if not absorption_short.empty else 0
    total_imbalance = len(imbalance) if not imbalance.empty else 0

    # Calcular métricas de backtest
    if total_long > 0:
        metrics_long = calculate_backtest_metrics(absorption_long)
    else:
        metrics_long = {}

    if total_short > 0:
        metrics_short = calculate_backtest_metrics(absorption_short)
    else:
        metrics_short = {}

    html += "<div>"
    html += f"""
        <div class="metric">
            <div class="metric-value">{total_long}</div>
            <div class="metric-label">Absorcion Long</div>
        </div>
        <div class="metric">
            <div class="metric-value">{total_short}</div>
            <div class="metric-label">Absorcion Short</div>
        </div>
        <div class="metric">
            <div class="metric-value">{total_imbalance}</div>
            <div class="metric-label">Imbalance Total</div>
        </div>
    """
    html += "</div>"

    # Métricas de backtest Absorción Long
    if metrics_long:
        html += f"""
        <h2>Backtest Metrics - Absorcion LONG</h2>
        <div>
            <div class="metric"><div class="metric-value">{metrics_long.get('win_rate', 0):.1%}</div><div class="metric-label">Win Rate</div></div>
            <div class="metric"><div class="metric-value">{metrics_long.get('profit_factor', 0):.2f}</div><div class="metric-label">Profit Factor</div></div>
            <div class="metric"><div class="metric-value">{metrics_long.get('expectancy', 0):.2f}</div><div class="metric-label">Expectancy</div></div>
            <div class="metric"><div class="metric-value">{metrics_long.get('max_drawdown', 0):.2f}</div><div class="metric-label">Max Drawdown</div></div>
            <div class="metric"><div class="metric-value">{metrics_long.get('avg_mfe', 0):.2f}</div><div class="metric-label">Avg MFE</div></div>
            <div class="metric"><div class="metric-value">{metrics_long.get('avg_mae', 0):.2f}</div><div class="metric-label">Avg MAE</div></div>
        </div>
        <p>Exit Reasons: {metrics_long.get('exit_reasons', {})}</p>
        """

    # Métricas de backtest Absorción Short
    if metrics_short:
        html += f"""
        <h2>Backtest Metrics - Absorcion SHORT</h2>
        <div>
            <div class="metric"><div class="metric-value">{metrics_short.get('win_rate', 0):.1%}</div><div class="metric-label">Win Rate</div></div>
            <div class="metric"><div class="metric-value">{metrics_short.get('profit_factor', 0):.2f}</div><div class="metric-label">Profit Factor</div></div>
            <div class="metric"><div class="metric-value">{metrics_short.get('expectancy', 0):.2f}</div><div class="metric-label">Expectancy</div></div>
            <div class="metric"><div class="metric-value">{metrics_short.get('max_drawdown', 0):.2f}</div><div class="metric-label">Max Drawdown</div></div>
            <div class="metric"><div class="metric-value">{metrics_short.get('avg_mfe', 0):.2f}</div><div class="metric-label">Avg MFE</div></div>
            <div class="metric"><div class="metric-value">{metrics_short.get('avg_mae', 0):.2f}</div><div class="metric-label">Avg MAE</div></div>
        </div>
        <p>Exit Reasons: {metrics_short.get('exit_reasons', {})}</p>
        """

    # Tabla detallada Absorción Long
    if not absorption_long.empty:
        html += "<h2>Absorcion LONG (" + str(len(absorption_long)) + " señales)</h2>"
        html += "<p><strong>Filtro aplicado:</strong> Solo señales en UPTREND o cerca de soporte (last_swing_low)</p>"
        cols = ['timestamp', 'price_close', 'entry_trigger', 'stop_loss', 'target',
                'trend_direction', 'last_swing_low', 'market_structure_event',
                'exit_time', 'exit_price', 'exit_reason', 'pnl', 'max_profit', 'max_loss', 'mfe', 'mae']
        available_cols = [c for c in cols if c in absorption_long.columns]

        # Formatear columnas
        formatters = {
            'timestamp': lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else 'N/A',
            'exit_time': lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else 'N/A',
            'price_close': '{:.2f}'.format,
            'entry_trigger': '{:.2f}'.format,
            'stop_loss': '{:.2f}'.format,
            'target': '{:.2f}'.format,
            'exit_price': '{:.2f}'.format,
            'pnl': '{:+.2f}'.format,
            'max_profit': '{:+.2f}'.format,
            'max_loss': '{:+.2f}'.format,
            'mfe': '{:+.2f}'.format,
            'mae': '{:+.2f}'.format,
        }

        # Aplicar color a PnL
        def pnl_color(val):
            if val > 0:
                return 'color: #00ff88'
            elif val < 0:
                return 'color: #ff4444'
            return ''

        html += absorption_long[available_cols].to_html(
            index=False,
            classes='data-table',
            border=0,
            formatters=formatters
        )

    # Tabla detallada Absorción Short
    if not absorption_short.empty:
        html += "<h2>Absorcion SHORT (" + str(len(absorption_short)) + " señales)</h2>"
        html += "<p><strong>Filtro aplicado:</strong> Solo señales en DOWNTREND o cerca de resistencia (last_swing_high)</p>"
        cols = ['timestamp', 'price_close', 'entry_trigger', 'stop_loss', 'target',
                'trend_direction', 'last_swing_high', 'market_structure_event',
                'exit_time', 'exit_price', 'exit_reason', 'pnl', 'max_profit', 'max_loss', 'mfe', 'mae']
        available_cols = [c for c in cols if c in absorption_short.columns]

        formatters = {
            'timestamp': lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else 'N/A',
            'exit_time': lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else 'N/A',
            'price_close': '{:.2f}'.format,
            'entry_trigger': '{:.2f}'.format,
            'stop_loss': '{:.2f}'.format,
            'target': '{:.2f}'.format,
            'exit_price': '{:.2f}'.format,
            'pnl': '{:+.2f}'.format,
            'max_profit': '{:+.2f}'.format,
            'max_loss': '{:+.2f}'.format,
            'mfe': '{:+.2f}'.format,
            'mae': '{:+.2f}'.format,
        }

        html += absorption_short[available_cols].to_html(
            index=False,
            classes='data-table',
            border=0,
            formatters=formatters
        )

    # Tabla Imbalance
    if not imbalance.empty:
        html += "<h2>Imbalance (" + str(len(imbalance)) + " señales)</h2>"
        imb_long = imbalance[imbalance['type'] == 'IMBALANCE_LONG']
        imb_short = imbalance[imbalance['type'] == 'IMBALANCE_SHORT']

        if not imb_long.empty:
            html += "<h3>Imbalance LONG</h3>"
            cols = ['timestamp', 'price_bin', 'imbalance_ratio', 'streak',
                    'exit_time', 'exit_reason', 'pnl', 'mfe', 'mae']
            available_cols = [c for c in cols if c in imb_long.columns]

            formatters = {
                'timestamp': lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else 'N/A',
                'exit_time': lambda x: x.strftime('%Y-%m-%d %H:%M') if pd.notna(x) else 'N/A',
                'price_bin': '{:.2f}'.format,
                'imbalance_ratio': '{:.2f}'.format,
                'pnl': '{:+.2f}'.format,
                'mfe': '{:+.2f}'.format,
                'mae': '{:+.2f}'.format,
            }

            html += imb_long[available_cols].to_html(index=False, classes='data-table', border=0, formatters=formatters)

        if not imb_short.empty:
            html += "<h3>Imbalance SHORT</h3>"
            cols = ['timestamp', 'price_bin', 'imbalance_ratio', 'streak',
                    'exit_time', 'exit_reason', 'pnl', 'mfe', 'mae']
            available_cols = [c for c in cols if c in imb_short.columns]

            html += imb_short[available_cols].to_html(index=False, classes='data-table', border=0, formatters=formatters)

    html += """
    </body>
    </html>
    """

    return html


# ==================== MAIN ====================

def main():
    print("=" * 60)
    print("STRATEGY SCANNER - Order Flow Patterns + Backtest")
    print("=" * 60)
    print(f"Symbol: {SYMBOL}")
    print(f"Periodo: {START_DATE} a {END_DATE}")
    print(f"Intervalo: {INTERVAL}")
    print(f"RR Ratio: {RR_RATIO}:1 | Max Candiles: {MAX_CANDLES_EXIT}")
    print("=" * 60)

    # Conectar a la DB
    print("\n[1/6] Conectando a la base de datos...")
    db = AggTradeDB(RAW_PATH, ANALYTICS_PATH, read_only=True)

    # Obtener datos OHLC
    print("[2/6] Cargando datos OHLC...")
    df_ohlc = db.get_ohlc(INTERVAL, START_DATE, END_DATE)
    print(f"      [OK] {len(df_ohlc)} velas cargadas")

    # Obtener volume profile
    print("[3/6] Cargando Volume Profile...")
    df_vol_profile = db.get_volume_profile(INTERVAL, START_DATE, END_DATE, resolution=10)
    print(f"      [OK] {len(df_vol_profile)} bins cargados")

    # Calcular columnas adicionales necesarias
    print("[4/6] Calculando indicadores...")
    window = 20
    df_ohlc['volume_ma'] = df_ohlc['volume'].rolling(window=window).mean()
    df_ohlc['volume_high'] = df_ohlc['volume'] >= df_ohlc['volume_ma'] * ABSORPTION_VOLUME_MA_MULT
    df_ohlc['delta_normalized'] = df_ohlc['buy_volume'] / (df_ohlc['volume'] + 1e-9)
    df_ohlc['delta_ma'] = df_ohlc['delta'].rolling(window=window).sum()

    # Obtener market context para filtro de eficiencia y Market Structure
    interval_name = INTERVAL.replace(" ", "_")
    try:
        df_context = db.con.execute(f"""
            SELECT * FROM analytics.market_context_{interval_name}
            WHERE open_time >= '{START_DATE}' AND open_time <= '{END_DATE}'
            ORDER BY open_time
        """).fetchdf()
        df_ohlc = df_ohlc.merge(
            df_context[['open_time', 'efficiency_normalized', 'efficiency_ma',
                       'last_swing_high', 'last_swing_low', 'market_structure_event',
                       'trend_direction', 'bars_since_structure']],
            on='open_time',
            how='left'
        )
        print("      [OK] Market context mergeado (con Market Structure)")
    except Exception as e:
        print(f"      [WARN] Market context no disponible: {e}")
        df_ohlc['efficiency_normalized'] = np.nan
        df_ohlc['efficiency_ma'] = np.nan
        df_ohlc['last_swing_high'] = np.nan
        df_ohlc['last_swing_low'] = np.nan
        df_ohlc['market_structure_event'] = None
        df_ohlc['trend_direction'] = None
        df_ohlc['bars_since_structure'] = np.nan

    # Cerrar conexión DB antes del backtest
    db.close_connection()

    # Detectar señales
    print("[5/6] Escaneando patrones...")

    print("      [SCAN] Buscando Absorcion LONG...")
    absorption_long_raw = detect_absorption_long(df_ohlc)
    print(f"        Encontradas: {len(absorption_long_raw)} señales")

    print("      [SCAN] Buscando Absorcion SHORT...")
    absorption_short_raw = detect_absorption_short(df_ohlc)
    print(f"        Encontradas: {len(absorption_short_raw)} señales")

    print("      [SCAN] Buscando Imbalance...")
    imbalance_raw = detect_imbalance_signals(df_vol_profile, df_ohlc)
    imb_long = len(imbalance_raw[imbalance_raw['type'] == 'IMBALANCE_LONG'])
    imb_short = len(imbalance_raw[imbalance_raw['type'] == 'IMBALANCE_SHORT'])
    print(f"        Encontradas: {len(imbalance_raw)} señales ({imb_long} long, {imb_short} short)")

    # Ejecutar backtest en las señales
    print("[6/6] Ejecutando backtest en las señales...")

    print("      [BACKTEST] Absorcion LONG...")
    absorption_long = run_backtest_on_signals(absorption_long_raw, df_ohlc)

    print("      [BACKTEST] Absorcion SHORT...")
    absorption_short = run_backtest_on_signals(absorption_short_raw, df_ohlc)

    print("      [BACKTEST] Imbalance...")
    imbalance = run_backtest_on_signals(imbalance_raw, df_ohlc)

    # Calcular métricas de backtest
    print("\n" + "=" * 60)
    print("METRICAS DE BACKTEST")
    print("=" * 60)

    if not absorption_long.empty:
        metrics = calculate_backtest_metrics(absorption_long)
        print(f"\n[ABSORCION LONG] ({metrics['total_trades']} trades)")
        print(f"  Win Rate: {metrics['win_rate']:.1%}")
        print(f"  Profit Factor: {metrics['profit_factor']:.2f}")
        print(f"  Expectancy: {metrics['expectancy']:.2f}")
        print(f"  Max Drawdown: {metrics['max_drawdown']:.2f}")
        print(f"  Avg MFE: {metrics['avg_mfe']:.2f} | Avg MAE: {metrics['avg_mae']:.2f}")
        print(f"  Exit Reasons: {metrics['exit_reasons']}")

    if not absorption_short.empty:
        metrics = calculate_backtest_metrics(absorption_short)
        print(f"\n[ABSORCION SHORT] ({metrics['total_trades']} trades)")
        print(f"  Win Rate: {metrics['win_rate']:.1%}")
        print(f"  Profit Factor: {metrics['profit_factor']:.2f}")
        print(f"  Expectancy: {metrics['expectancy']:.2f}")
        print(f"  Max Drawdown: {metrics['max_drawdown']:.2f}")
        print(f"  Avg MFE: {metrics['avg_mfe']:.2f} | Avg MAE: {metrics['avg_mae']:.2f}")
        print(f"  Exit Reasons: {metrics['exit_reasons']}")

    if not imbalance.empty:
        metrics_long = calculate_backtest_metrics(imbalance[imbalance['type'] == 'IMBALANCE_LONG'])
        metrics_short = calculate_backtest_metrics(imbalance[imbalance['type'] == 'IMBALANCE_SHORT'])

        if metrics_long:
            print(f"\n[IMBALANCE LONG] ({metrics_long['total_trades']} trades)")
            print(f"  Win Rate: {metrics_long['win_rate']:.1%}")
            print(f"  Profit Factor: {metrics_long['profit_factor']:.2f}")

        if metrics_short:
            print(f"\n[IMBALANCE SHORT] ({metrics_short['total_trades']} trades)")
            print(f"  Win Rate: {metrics_short['win_rate']:.1%}")
            print(f"  Profit Factor: {metrics_short['profit_factor']:.2f}")

    # Guardar resultados
    print("\n" + "=" * 60)
    print("GUARDANDO RESULTADOS")
    print("=" * 60)

    # CSVs individuales
    absorption_long.to_csv(f'{OUTPUT_DIR}/absorption_long.csv', index=False)
    print(f"[OK] {OUTPUT_DIR}/absorption_long.csv ({len(absorption_long)} filas)")

    absorption_short.to_csv(f'{OUTPUT_DIR}/absorption_short.csv', index=False)
    print(f"[OK] {OUTPUT_DIR}/absorption_short.csv ({len(absorption_short)} filas)")

    imbalance.to_csv(f'{OUTPUT_DIR}/imbalance.csv', index=False)
    print(f"[OK] {OUTPUT_DIR}/imbalance.csv ({len(imbalance)} filas)")

    # Resumen combinado
    all_signals = pd.concat([absorption_long, absorption_short, imbalance], ignore_index=True)
    if not all_signals.empty:
        all_signals = all_signals.sort_values('timestamp')
        all_signals.to_csv(f'{OUTPUT_DIR}/all_signals.csv', index=False)
        print(f"[OK] {OUTPUT_DIR}/all_signals.csv ({len(all_signals)} señales totales)")

    # HTML resumen
    html_content = generate_summary_html(absorption_long, absorption_short, imbalance)
    with open(f'{OUTPUT_DIR}/scanner_summary.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"[OK] {OUTPUT_DIR}/scanner_summary.html")

    # Imprimir algunas señales de ejemplo
    print("\n" + "=" * 60)
    print("EJEMPLOS DE SEÑALES CON RESULTADO")
    print("=" * 60)

    if not absorption_long.empty:
        print("\n[ABSORCION LONG] Primeras 5 señales con resultado:")
        cols = ['timestamp', 'price_close', 'exit_reason', 'pnl', 'mfe', 'mae']
        print(absorption_long[cols].head().to_string(index=False))

    if not absorption_short.empty:
        print("\n[ABSORCION SHORT] Primeras 5 señales con resultado:")
        cols = ['timestamp', 'price_close', 'exit_reason', 'pnl', 'mfe', 'mae']
        print(absorption_short[cols].head().to_string(index=False))

    print("\n" + "=" * 60)
    print("Scanner completado. Revisa la carpeta", OUTPUT_DIR)
    print("=" * 60)


if __name__ == '__main__':
    main()
