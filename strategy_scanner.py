"""
Strategy Scanner - Escanea patrones de Order Flow en la base de datos

Escanea estrategias

Genera:
- Tablas CSV con cada señal encontrada
- Backtest de cada señal (TOUCH_TARGET, TOUCH_STOP, MAX_PROFIT, MAX_LOSS)
- HTML filtrado por estrategia para revisión visual
"""

import pandas as pd
import numpy as np
from analytics_db import AnalyticsDB
from base_strategy import backtest_signals, calculate_backtest_metrics
from strategies import calculate_candle_proportions, check_trend_filter, AbsorcionLong, AbsorcionShort, DeltaDivergence
from strategy_charts import StrategyChart
import os

# ==================== CONFIGURACIÓN ====================

SYMBOL = 'BTCUSDT'
RAW_PATH = f'data/{SYMBOL}/tradebook/raw_data.db'
ANALYTICS_PATH = f'data/{SYMBOL}/tradebook/analytics.db'

# Rango de fechas para escanear
START_DATE = '2026-01-01'
END_DATE = '2026-05-08'
INTERVAL = '5 minutes'
INTERVAL_CONTEXT = '4 hours'  # Para calcular market structure y tendencia general

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
    db = AnalyticsDB(ANALYTICS_PATH)

    # Obtener datos OHLC
    print("[2/6] Cargando datos OHLC...")
    df_ohlc = db.get_ohlc(INTERVAL, START_DATE, END_DATE)
    print(f"      [OK] {len(df_ohlc)} velas cargadas")

    # Obtener volume profile
    print("[3/6] Cargando Volume Profile...")
    df_vol_profile = db.get_volume_profile(INTERVAL, START_DATE, END_DATE, resolution=10)
    df_ohlc['poc'] = df_vol_profile[df_vol_profile['is_poc'] == True]['price_bin'].values
    print(f"      [OK] {len(df_vol_profile)} bins cargados")
  
    # Calcular columnas adicionales necesarias
    print("[4/6] Calculando indicadores...")
    window = 20
    df_ohlc['volume_ma'] = df_ohlc['volume'].rolling(window=window).mean()
    df_ohlc['volume_high'] = df_ohlc['volume'] >= df_ohlc['volume_ma'] * ABSORPTION_VOLUME_MA_MULT
    df_ohlc['delta_normalized'] = df_ohlc['buy_volume'] / (df_ohlc['volume'] + 1e-9)
    df_ohlc['delta_ma'] = df_ohlc['delta'].rolling(window=window).sum()

    # Obtener market context para filtro de eficiencia y Market Structure
    try:
        df_context = db.get_market_context(INTERVAL_CONTEXT, START_DATE, END_DATE)
        df_ohlc = pd.merge_asof(
            df_ohlc,
            df_context[['open_time', 'trend', 'sh_type', 'sl_type',
                        'last_sh_level', 'last_sh_type', 'last_sl_level', 'last_sl_type']],
            on='open_time',
            direction='backward'
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
    absorption_long = AbsorcionLong()
    absorption_long_raw = absorption_long.scan(df_ohlc)
    print(f"        Encontradas: {len(absorption_long_raw)} señales")
    
    print("      [SCAN] Buscando Absorcion SHORT...")
    absorption_short = AbsorcionShort()
    absorption_short_raw = absorption_short.scan(df_ohlc)
    print(f"        Encontradas: {len(absorption_short_raw)} señales")

    print("      [SCAN] Buscando Delta divergence...")
    delta_divergence = DeltaDivergence()
    delta_divergence_raw = delta_divergence.scan(df_ohlc)
    print(f"        Encontradas: {len(delta_divergence_raw)} señales")

    # Ejecutar backtest en las señales
    print("[6/6] Ejecutando backtest en las señales...")

    print("      [BACKTEST] Absorcion LONG...")
    absorption_long = backtest_signals(absorption_long_raw, df_ohlc, rr_ratio=RR_RATIO, max_candles=MAX_CANDLES_EXIT)

    print("      [BACKTEST] Absorcion SHORT...")
    absorption_short = backtest_signals(absorption_short_raw, df_ohlc, rr_ratio=RR_RATIO, max_candles=MAX_CANDLES_EXIT)

    print("      [BACKTEST] Delta divergence...")
    delta_divergence = backtest_signals(delta_divergence_raw, df_ohlc, rr_ratio=RR_RATIO, max_candles=MAX_CANDLES_EXIT)
    
    # Graficar strategia
    
    
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

    if not delta_divergence.empty:
        metrics = calculate_backtest_metrics(delta_divergence)
        print(f"\n[DELTA DIVERGENCE] ({metrics['total_trades']} trades)")
        print(f"  Win Rate: {metrics['win_rate']:.1%}")
        print(f"  Profit Factor: {metrics['profit_factor']:.2f}")
        print(f"  Expectancy: {metrics['expectancy']:.2f}")
        print(f"  Max Drawdown: {metrics['max_drawdown']:.2f}")
        print(f"  Avg MFE: {metrics['avg_mfe']:.2f} | Avg MAE: {metrics['avg_mae']:.2f}")
        print(f"  Exit Reasons: {metrics['exit_reasons']}")
    # Graficar estrategias
    strategy_chart = StrategyChart("Absorcion_Long", absorption_long_raw, df_ohlc)
    strategy_chart.create_chart()
    
    # Guardar resultados
    print("\n" + "=" * 60)
    print("GUARDANDO RESULTADOS")
    print("=" * 60)

    # CSVs individuales
    absorption_long.to_csv(f'{OUTPUT_DIR}/absorption_long.csv', index=False)
    print(f"[OK] {OUTPUT_DIR}/absorption_long.csv ({len(absorption_long)} filas)")

    absorption_short.to_csv(f'{OUTPUT_DIR}/absorption_short.csv', index=False)
    print(f"[OK] {OUTPUT_DIR}/absorption_short.csv ({len(absorption_short)} filas)")

    delta_divergence.to_csv(f'{OUTPUT_DIR}/delta_divergence.csv', index=False)
    print(f"[OK] {OUTPUT_DIR}/delta_divergence.csv ({len(delta_divergence)} filas)")

    # Resumen combinado
    all_signals = pd.concat([absorption_long, absorption_short, delta_divergence], ignore_index=True)
    if not all_signals.empty:
        all_signals = all_signals.sort_values('timestamp')
        all_signals.to_csv(f'{OUTPUT_DIR}/all_signals.csv', index=False)
        print(f"[OK] {OUTPUT_DIR}/all_signals.csv ({len(all_signals)} señales totales)")

    # HTML resumen
    html_content = generate_summary_html(absorption_long, absorption_short, delta_divergence)
    with open(f'{OUTPUT_DIR}/scanner_summary.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"[OK] {OUTPUT_DIR}/scanner_summary.html")

if __name__ == '__main__':
    main()
