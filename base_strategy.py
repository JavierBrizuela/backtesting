"""
Strategy Framework - Base classes and shared utilities for backtesting

Usage:
    from base_strategy import Strategy, Signal, backtest_signals, metrics_report

    class MyStrategy(Strategy):
        name = "Mi Estrategia"
        params = {"volume_mult": 1.5, "rr_ratio": 2.0}

        def scan(self, df, **kwargs):
            # return list of Signal objects
            pass

    strategy = MyStrategy()
    signals = strategy.scan(df_ohlc)
    results = backtest_signals(signals, df_ohlc, **strategy.params)
    print(metrics_report(results))
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
import numpy as np


@dataclass
class Signal:
    """Representa una senal de trading generada por una estrategia."""
    timestamp: pd.Timestamp
    type: str
    direction: str  # 'LONG' or 'SHORT'
    entry_trigger: float
    stop_loss: float
    target: float
    price_open: float = 0.0
    price_high: float = 0.0
    price_low: float = 0.0
    price_close: float = 0.0
    volume: float = 0.0
    entry_mode: str = 'IMMEDIATE'  # 'IMMEDIATE' (entry at trigger on next candle) or 'LIMIT' (wait for price to be reached)
    metadata: Dict = field(default_factory=dict)

    def to_dict(self):
        d = {
            'timestamp': self.timestamp,
            'type': self.type,
            'direction': self.direction,
            'entry_trigger': self.entry_trigger,
            'stop_loss': self.stop_loss,
            'target': self.target,
            'price_open': self.price_open,
            'price_high': self.price_high,
            'price_low': self.price_low,
            'price_close': self.price_close,
        }
        d.update(self.metadata)
        return d


@dataclass
class TradeResult:
    """Resultado de un trade simulado."""
    exit_time: Optional[pd.Timestamp]
    exit_price: Optional[float]
    exit_reason: str
    pnl: float
    mae: float  # Maximum Adverse Excursion
    mfe: float  # Maximum Favorable Excursion
    fill_time: Optional[pd.Timestamp] = None  # Cuando se lleno la orden (LIMIT)
    fill_price: Optional[float] = None  # A qué precio se lleno (LIMIT)


class Strategy(ABC):
    """Clase base para todas las estrategias."""

    name: str = "BaseStrategy"
    # Parametros por defecto, cada estrategia redefine esto
    params: Dict = {}

    def __init__(self, **overrides):
        self.params = {**self.params, **overrides}

    @abstractmethod
    def scan(self, df: pd.DataFrame, **kwargs) -> List[Signal]:
        """
        Escanea el dataframe OHLC y retorna lista de senales.

        Args:
            df: DataFrame con columnas OHLCV + indicator columns mergeadas
            **kwargs: params extra (volume profile, market context, etc)

        Returns:
            List[Signal]
        """
        pass

    def get_param(self, key, default=None):
        return self.params.get(key, default)


# ==================== BACKTEST ENGINE ====================

def simulate_trade(signal: Signal, df_ohlc: pd.DataFrame, rr_ratio: float = 2.0, max_candles: int = 20) -> TradeResult:
    """Simula una operacion desde la senal hasta target/stop/expiracion."""
    signal_idx = df_ohlc[df_ohlc['open_time'] == signal.timestamp]
    if signal_idx.empty:
        return TradeResult(None, None, 'SIGNAL_NOT_FOUND', 0, 0, 0)
    signal_idx = signal_idx.index[0]
    future = df_ohlc.iloc[signal_idx + 1:signal_idx + 1 + max_candles].copy()
    if future.empty:
        return TradeResult(None, None, 'NO_DATA', 0, 0, 0)

    if signal.entry_mode == 'LIMIT':
        return _simulate_limit(signal, future, rr_ratio)
    else:
        return _simulate_immediate(signal, future, rr_ratio)


def _simulate_immediate(signal: Signal, future: pd.DataFrame, rr_ratio: float) -> TradeResult:
    """Entrada inmediata: asume que entramos al entry_trigger al inicio."""
    entry = signal.entry_trigger
    stop = signal.stop_loss

    if signal.direction == 'LONG':
        risk = entry - stop
        target = entry + risk * rr_ratio
        max_high = future['high'].max()
        min_low = future['low'].min()
        mfe = max_high - entry
        mae = entry - min_low

        for _, candle in future.iterrows():
            if candle['low'] <= stop:
                return TradeResult(candle['open_time'], stop, 'STOP_LOSS', -risk, mae, mfe)
            if candle['high'] >= target:
                return TradeResult(candle['open_time'], target, 'TARGET', risk * rr_ratio, mae, mfe)

        exit_price = future.iloc[-1]['close']
        return TradeResult(future.iloc[-1]['open_time'], exit_price, 'TIME_EXIT', exit_price - entry, mae, mfe)

    else:  # SHORT
        risk = stop - entry
        target = entry - risk * rr_ratio
        max_high = future['high'].max()
        min_low = future['low'].min()
        mfe = entry - min_low
        mae = max_high - entry

        for _, candle in future.iterrows():
            if candle['high'] >= stop:
                return TradeResult(candle['open_time'], stop, 'STOP_LOSS', -risk, mae, mfe)
            if candle['low'] <= target:
                return TradeResult(candle['open_time'], target, 'TARGET', risk * rr_ratio, mae, mfe)

        exit_price = future.iloc[-1]['close']
        return TradeResult(future.iloc[-1]['open_time'], exit_price, 'TIME_EXIT', entry - exit_price, mae, mfe)


def _simulate_limit(signal: Signal, future: pd.DataFrame, rr_ratio: float) -> TradeResult:
    """
    Entrada LIMIT: escanea velas futuras hasta que el mercado alcanza
    el entry_trigger. Solo ahi se activa el trade y se monitorea stop/target.
    Si no se llena en max_candles, retorna NOT_FILLED.
    """
    entry = signal.entry_trigger
    stop = signal.stop_loss

    if signal.direction == 'LONG':
        for i, candle in future.iterrows():
            if candle['low'] <= entry:
                fill_time = candle['open_time']
                fill_price = entry
                remaining = future.loc[future.index > i]

                if remaining.empty:
                    return TradeResult(fill_time, fill_price, 'TIME_EXIT', 0, 0, 0, fill_time, fill_price)

                risk = fill_price - stop
                target = fill_price + risk * rr_ratio

                max_high = remaining['high'].max()
                min_low = remaining['low'].min()
                mfe = max_high - fill_price
                mae = fill_price - min_low

                for _, c in remaining.iterrows():
                    if c['low'] <= stop:
                        return TradeResult(c['open_time'], stop, 'STOP_LOSS', -risk, mae, mfe, fill_time, fill_price)
                    if c['high'] >= target:
                        return TradeResult(c['open_time'], target, 'TARGET', risk * rr_ratio, mae, mfe, fill_time, fill_price)

                exit_price = remaining.iloc[-1]['close']
                pnl = exit_price - fill_price
                return TradeResult(remaining.iloc[-1]['open_time'], exit_price, 'TIME_EXIT', pnl, mae, mfe, fill_time, fill_price)

        return TradeResult(None, None, 'NOT_FILLED', 0, 0, 0)

    else:  # SHORT
        for i, candle in future.iterrows():
            if candle['high'] >= entry:
                fill_time = candle['open_time']
                fill_price = entry
                remaining = future.loc[future.index > i]

                if remaining.empty:
                    return TradeResult(fill_time, fill_price, 'TIME_EXIT', 0, 0, 0, fill_time, fill_price)

                risk = stop - fill_price
                target = fill_price - risk * rr_ratio

                max_high = remaining['high'].max()
                min_low = remaining['low'].min()
                mfe = fill_price - min_low
                mae = max_high - fill_price

                for _, c in remaining.iterrows():
                    if c['high'] >= stop:
                        return TradeResult(c['open_time'], stop, 'STOP_LOSS', -risk, mae, mfe, fill_time, fill_price)
                    if c['low'] <= target:
                        return TradeResult(c['open_time'], target, 'TARGET', risk * rr_ratio, mae, mfe, fill_time, fill_price)

                exit_price = remaining.iloc[-1]['close']
                pnl = fill_price - exit_price
                return TradeResult(remaining.iloc[-1]['open_time'], exit_price, 'TIME_EXIT', pnl, mae, mfe, fill_time, fill_price)

        return TradeResult(None, None, 'NOT_FILLED', 0, 0, 0)


def backtest_signals(signals: List[Signal], df_ohlc: pd.DataFrame, rr_ratio: float = 2.0, max_candles: int = 20) -> pd.DataFrame:
    """Ejecuta backtest sobre todas las senales y retorna DataFrame con resultados."""
    results = []
    for signal in signals:
        trade = simulate_trade(signal, df_ohlc, rr_ratio, max_candles)
        results.append({
            **signal.to_dict(),
            'exit_time': trade.exit_time,
            'exit_price': trade.exit_price,
            'exit_reason': trade.exit_reason,
            'pnl': trade.pnl,
            'mae': trade.mae,
            'mfe': trade.mfe,
            'fill_time': trade.fill_time,
            'fill_price': trade.fill_price,
        })
    return pd.DataFrame(results)


def calculate_backtest_metrics(trades_df: pd.DataFrame) -> Dict:
    """Calcula metricas agregadas de backtest."""
    if trades_df.empty:
        return {}

    total = len(trades_df)
    wins = trades_df[trades_df['pnl'] > 0]
    losses = trades_df[trades_df['pnl'] <= 0]

    win_rate = len(wins) / total if total > 0 else 0
    avg_win = wins['pnl'].mean() if len(wins) > 0 else 0
    avg_loss = abs(losses['pnl'].mean()) if len(losses) > 0 else 0
    gross_profit = wins['pnl'].sum() if len(wins) > 0 else 0
    gross_loss = abs(losses['pnl'].sum()) if len(losses) > 0 else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    df_copy = trades_df.copy()
    df_copy['cum_pnl'] = df_copy['pnl'].cumsum()
    running_max = df_copy['cum_pnl'].cummax()
    drawdown = running_max - df_copy['cum_pnl']
    max_drawdown = drawdown.max()

    # Drawdown en porcentaje (sobre PnL acumulado maximo)
    max_cum = df_copy['cum_pnl'].max()
    drawdown_pct = (drawdown / max_cum * 100).max() if max_cum > 0 else 0

    # Consecutive losses
    loss_mask = df_copy['pnl'] <= 0
    consecutive = 0
    max_consecutive = 0
    for val in loss_mask:
        if val:
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0

    # Time in drawdown (proporcion de trades con cumsum < running max)
    time_in_dd = (drawdown > 0).mean()

    return {
        'total_trades': total,
        'winning_trades': len(wins),
        'losing_trades': len(losses),
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_factor': profit_factor,
        'expectancy': expectancy,
        'max_drawdown': max_drawdown,
        'max_drawdown_pct': drawdown_pct,
        'max_consecutive_losses': max_consecutive,
        'time_in_drawdown': time_in_dd,
        'avg_mfe': df_copy['mfe'].mean(),
        'avg_mae': df_copy['mae'].mean(),
        'exit_reasons': df_copy['exit_reason'].value_counts().to_dict(),
    }


def metrics_report(metrics: Dict, strategy_name: str = "") -> str:
    """Formatea las metricas en texto legible."""
    if not metrics or (isinstance(metrics, dict) and not metrics):
        return f"[{strategy_name}] No hay trades"

    lines = [
        f"{'='*50}",
        f"  {strategy_name or 'Strategy'} — Backtest Metrics",
        f"{'='*50}",
        f"  Total Trades:          {metrics['total_trades']}",
        f"  Win Rate:              {metrics['win_rate']:.1%}",
        f"  Avg Win:               {metrics['avg_win']:.2f}",
        f"  Avg Loss:              {metrics['avg_loss']:.2f}",
        f"  Profit Factor:         {metrics['profit_factor']:.2f}",
        f"  Expectancy:            {metrics['expectancy']:.2f}",
        f"  Max Drawdown:          {metrics['max_drawdown']:.2f} ({metrics.get('max_drawdown_pct', 0):.1f}%)",
        f"  Max Consec Losses:     {metrics['max_consecutive_losses']}",
        f"  Time in Drawdown:      {metrics['time_in_drawdown']:.1%}",
        f"  Avg MFE:               {metrics['avg_mfe']:.2f}",
        f"  Avg MAE:               {metrics['avg_mae']:.2f}",
        f"  Exit Reasons:          {metrics.get('exit_reasons', {})}",
        f"{'='*50}",
    ]
    return '\n'.join(lines)
