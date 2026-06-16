import pandas as pd
from itertools import product
from typing import Dict
import os

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
                    row[k] = v
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
            print(f"  [{i+1}/{total}] ({progress:.0f}%) {params} >> {trades} trades  ", end='\r')

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
        param_cols = [c for c in df.columns if c not in cols]
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

    def best(self, results: pd.DataFrame, sort_by='profit_factor', min_trades=10):
        """Retorna la mejor combinacion con al menos min_trades."""
        filtered = results[results['total_trades'] >= min_trades]
        if filtered.empty:
            return None
        return filtered.sort_values(sort_by, ascending=False).iloc[0]
