"""
Parameter Scanner - Grid search de parametros para cualquier estrategia

Uso:
    # En un script:
    from agg_trades_to_db import AggTradeDB
    from base_strategy import backtest_signals, metrics_report
    from strategies import AbsorcionLong
    from param_scan import ParamScan

    db = AggTradeDB(RAW_PATH, ANALYTICS_PATH, read_only=True)
    df = db.get_ohlc('15 minutes', '2026-01-01', '2026-03-01')
    # preparar indicadores...
    db.close()

    scan = ParamScan()
    results = scan.run(
        strategy_cls=AbsorcionLong,
        df=df,
        param_grid={
            'rr_ratio': [1.5, 2.0, 2.5],
            'delta_thresh': [0.40, 0.46, 0.50],
            'volume_mult': [1.5, 1.8, 2.0],
        },
    )
    scan.print_table(results)
    scan.save_csv(results, 'param_scan_results.csv')

    # O desde linea de comandos:
    pipenv run python param_scan.py
"""

import pandas as pd
import numpy as np
from itertools import product
from typing import Dict, List
import os

SYMBOL = 'BTCUSDT'
RAW_PATH = f'data/{SYMBOL}/tradebook/raw_data.db'
ANALYTICS_PATH = f'data/{SYMBOL}/tradebook/analytics.db'

OUTPUT_DIR = 'scanner_output'
os.makedirs(OUTPUT_DIR, exist_ok=True)


class ParamScan:
    """Ejecuta grid search de parametros sobre una estrategia."""

    def run(self, strategy_cls, df: pd.DataFrame, param_grid: Dict, **scan_kwargs) -> pd.DataFrame:
        """
        Ejecuta todas las combinaciones de parametros y retorna resultados.

        Args:
            strategy_cls: clase Strategy
            df: DataFrame OHLC preparado (con indicadores)
            param_grid: dict de {param: [valores]}
            **scan_kwargs: kwargs extra para scan() (ej: df_vol_profile)
        """
        keys = list(param_grid.keys())
        values = list(param_grid.values())
        combinations = list(product(*values))

        results = []
        total = len(combinations)

        for i, combo in enumerate(combinations):
            params = dict(zip(keys, combo))
            strategy = strategy_cls(**params)
            signals = strategy.scan(df, **scan_kwargs)

            rr = params.get('rr_ratio', 2.0)
            max_c = params.get('max_candles', 20)

            from base_strategy import backtest_signals, calculate_backtest_metrics
            bt_df = backtest_signals(signals, df, rr_ratio=rr, max_candles=max_c)
            metrics = calculate_backtest_metrics(bt_df)

            if metrics:
                row = {'strategy': strategy_cls.name}
                for k, v in params.items():
                    row[f'param_{k}'] = v
                row['total_trades'] = metrics['total_trades']
                row['win_rate'] = metrics['win_rate']
                row['profit_factor'] = metrics['profit_factor']
                row['expectancy'] = metrics['expectancy']
                row['max_drawdown'] = metrics['max_drawdown']
                row['max_drawdown_pct'] = metrics.get('max_drawdown_pct', 0)
                row['avg_mfe'] = metrics['avg_mfe']
                row['avg_mae'] = metrics['avg_mae']
                results.append(row)

            progress = (i + 1) / total * 100
            trades = metrics['total_trades'] if metrics else 0
            print(f"  [{i+1}/{total}] ({progress:.0f}%) {params} >> {trades} trades", end='\r')

        print()  # newline after progress
        return pd.DataFrame(results)

    def print_table(self, results: pd.DataFrame, sort_by='profit_factor', ascending=False):
        """Imprime tabla ordenada de resultados."""
        if results.empty:
            print("No results.")
            return

        df = results.sort_values(sort_by, ascending=ascending).reset_index(drop=True)

        cols = ['strategy', 'total_trades', 'win_rate', 'profit_factor', 'expectancy', 'max_drawdown_pct']
        cols = [c for c in cols if c in df.columns]
        # Agregar columnas de parametros
        param_cols = [c for c in df.columns if c.startswith('param_')]
        display_cols = param_cols + cols

        print(f"\n{'='*100}")
        print(f"  PARAMETER SCAN -- {len(df)} combinaciones -- ordenado por {sort_by}")
        print(f"{'='*100}")

        for c in display_cols:
            if c == 'strategy':
                continue
            if c in df.columns:
                print(f"  {c:>30s}: {df[c].iloc[0]}")
        print(f"{'='*100}")
        print(df[display_cols].head(20).to_string(index=True))
        print(f"\n... ({len(df)} total rows, showing top 20)")

    def save_csv(self, results: pd.DataFrame, filename: str):
        path = os.path.join(OUTPUT_DIR, filename)
        results.to_csv(path, index=False)
        print(f"[OK] {path} ({len(results)} rows)")

    def best(self, results: pd.DataFrame, by='profit_factor', min_trades=10):
        """Retorna la mejor combinacion con al menos min_trades."""
        filtered = results[results['total_trades'] >= min_trades]
        if filtered.empty:
            return None
        return filtered.sort_values(by, ascending=False).iloc[0]


# ============================================================
# Runner desde linea de comandos
# ============================================================

def prepare_df(db, interval, start, end, volume_ma_window=20):
    """Prepara el DataFrame OHLC con indicadores para escaneo."""
    df = db.get_ohlc(interval, start, end)

    # Indicadores necesarios para las estrategias de absorcion
    df['volume_ma'] = df['volume'].rolling(window=volume_ma_window).mean()
    df['volume_high'] = df['volume'] >= df['volume_ma'] * 1.8
    df['delta_normalized'] = df['buy_volume'] / (df['volume'] + 1e-9)
    df['delta_ma'] = df['delta'].rolling(window=volume_ma_window).sum()

    # Agregar POC desde volume profile
    try:
        df_vol = db.get_volume_profile(interval, start, end, resolution=10)
        vp_summary = df_vol[df_vol['node_type'] == 'POC'][['open_time', 'price_bin']].copy()
        vp_summary.rename(columns={'price_bin': 'poc'}, inplace=True)
        df = df.merge(vp_summary, on='open_time', how='left')
    except Exception as e:
        print(f"  [WARN] No se pudo agregar POC: {e}")
        df['poc'] = np.nan

    # Market context: siempre en 4H (es la unica tabla creada por populate_DB.py)
    # Merge + forward-fill para que cada vela herede el contexto 4H mas reciente
    try:
        # Las columnas reales en la DB son: open_time, trend_direction, regime,
        # last_swing_high, last_swing_low, market_structure_event, bars_since_structure,
        # efficiency, efficiency_ratio, r_squared, coefficient_variation, atr_normalized, delta_efficiency
        df_context = db.con.execute(f"""
            SELECT open_time, trend_direction, regime,
                   last_swing_high, last_swing_low, market_structure_event,
                   bars_since_structure, efficiency, efficiency_ratio
            FROM analytics.market_context_4_hours
            ORDER BY open_time
        """).fetchdf()

        # Compute runtime columns (these aren't stored, computed by visualization code)
        eff_max = df_context['efficiency'].replace(0, np.nan).max()
        df_context['efficiency_normalized'] = df_context['efficiency'] / eff_max if eff_max > 0 else np.nan
        df_context['efficiency_ma'] = df_context['efficiency_normalized'].rolling(window=volume_ma_window).mean()

        df = df.merge(df_context[['open_time', 'trend_direction', 'regime',
                                   'last_swing_high', 'last_swing_low',
                                   'market_structure_event', 'bars_since_structure',
                                   'efficiency_normalized', 'efficiency_ma']],
                      on='open_time', how='left')

        # Forward-fill: las velas de 15m intermedias heredan el ultimo contexto 4H
        context_cols = ['trend_direction', 'regime', 'last_swing_high', 'last_swing_low',
                        'market_structure_event', 'bars_since_structure',
                        'efficiency_normalized', 'efficiency_ma']
        for c in context_cols:
            if c in df.columns:
                df[c] = df[c].ffill()
    except Exception as e:
        for col in ['trend_direction', 'last_swing_high', 'last_swing_low',
                     'efficiency_normalized', 'efficiency_ma', 'regime',
                     'market_structure_event', 'bars_since_structure']:
            if col not in df.columns:
                df[col] = np.nan

    return df


def main():
    from agg_trades_to_db import AggTradeDB
    from strategies import AbsorcionLong, AbsorcionShort, DeltaDivergence

    SCAN_INTERVAL = '15 minutes'
    SCAN_START = '2026-01-01'
    SCAN_END = '2026-03-01'

    print("=" * 60)
    print("PARAMETER SCAN")
    print(f"Symbol: {SYMBOL} | Interval: {SCAN_INTERVAL}")
    print(f"Period: {SCAN_START} to {SCAN_END}")
    print("=" * 60)

    # Conectar y preparar datos
    print("\n[1/4] Conectando a la base de datos...")
    db = AggTradeDB(RAW_PATH, ANALYTICS_PATH, read_only=True)

    print("[2/4] Cargando y preparando datos...")
    df = prepare_df(db, SCAN_INTERVAL, SCAN_START, SCAN_END)
    print(f"      [OK] {len(df)} velas cargadas")

    # Log trend distribution for debugging
    trend_counts = df['trend_direction'].value_counts(dropna=False) if 'trend_direction' in df.columns else {}
    print(f"      Trend distribution: {dict(trend_counts)}")

    db.close_connection()

    scan = ParamScan()
    all_results = []

    # ---- SCAN 1: Absorcion Long ----
    print("\n[3/4] Scan: Absorcion Long")
    results_long = scan.run(
        strategy_cls=AbsorcionLong,
        df=df,
        param_grid={
            'rr_ratio': [1.5, 2.0, 2.5],
            'delta_thresh': [0.40, 0.46, 0.50],
            'volume_mult': [1.5, 1.8, 2.0],
        },
    )
    if not results_long.empty:
        scan.print_table(results_long, sort_by='profit_factor')
        best = scan.best(results_long, by='profit_factor', min_trades=5)
        if best is not None:
            print(f"\n  MEJOR combinacion (PF={best['profit_factor']:.2f}, {best['total_trades']} trades):")
            for k, v in best.items():
                if k.startswith('param_'):
                    print(f"    {k}: {v}")
        all_results.append(results_long)

    # ---- SCAN 2: Absorcion Short ----
    print("\n[4/4] Scan: Absorcion Short")
    results_short = scan.run(
        strategy_cls=AbsorcionShort,
        df=df,
        param_grid={
            'rr_ratio': [1.5, 2.0, 2.5],
            'delta_thresh': [0.50, 0.54, 0.60],
            'volume_mult': [1.5, 1.8, 2.0],
        },
    )
    if not results_short.empty:
        scan.print_table(results_short, sort_by='profit_factor')
        best = scan.best(results_short, by='profit_factor', min_trades=5)
        if best is not None:
            print(f"\n  MEJOR combinacion (PF={best['profit_factor']:.2f}, {best['total_trades']} trades):")
            for k, v in best.items():
                if k.startswith('param_'):
                    print(f"    {k}: {v}")
        all_results.append(results_short)

    # ---- SCAN 3: Delta Divergence ----
    print("\n[5/5] Scan: Delta Divergence")
    results_div = scan.run(
        strategy_cls=DeltaDivergence,
        df=df,
        param_grid={
            'rr_ratio': [1.5, 2.0],
            'lookback': [15, 20, 30],
            'delta_min_change': [3.0, 5.0, 10.0],
        },
    )
    if not results_div.empty:
        scan.print_table(results_div, sort_by='profit_factor')
        best = scan.best(results_div, by='profit_factor', min_trades=5)
        if best is not None:
            print(f"\n  MEJOR combinacion (PF={best['profit_factor']:.2f}, {best['total_trades']} trades):")
            for k, v in best.items():
                if k.startswith('param_'):
                    print(f"    {k}: {v}")
        all_results.append(results_div)

    # Guardar resultados combinados
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        scan.save_csv(combined, 'param_scan_combined.csv')
        print("\n[OK] Todos los resultados guardados en scanner_output/")


if __name__ == '__main__':
    main()
