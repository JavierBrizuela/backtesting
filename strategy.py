from agg_trades_db import AggTradeDB
from backtesting import Backtest, Strategy
from backtesting.lib import FractionalBacktest, plot_heatmaps
import pandas as pd
import numpy as np
import os

interval = '5 minutes'
interval_trades = '5 minutes'
size_position = 1
resolution = 25
start = '2025-10-22'
end = '2025-10-28'
simbol = 'BTCUSDT'
db_path = f'data/{simbol}/tradebook/agg_trades.db'
path = 'bokeh_output'


# Consultas a la base de datos
db_connector = AggTradeDB(db_path)
df_ohlc = db_connector.get_ohlc(interval, start, end)
df_vol_profile = db_connector.get_volume_profile(interval, start, end, resolution)
db_connector.close_connection()
df_ohlc.set_index('open_time', inplace=True)
df_ohlc.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)

#df_ohlc['delta_cum'] = df_ohlc['delta'].cumsum()
#df_ohlc['delta_ma'] = df_ohlc['delta'].rolling(window=window[2]).sum()
def SMA(data, n):
    return pd.Series(data).rolling(n).mean()
print(df_vol_profile)
print(df_ohlc.head())
df_poc = df_vol_profile.groupby('open_time')['total_volume'].idxmax()
df_ohlc['poc'] = df_vol_profile.loc[df_poc, 'price_bin'].values / 100000000
df_ohlc['delta_normalized'] = df_ohlc['buy_volume'] / df_ohlc['Volume']
class signal_strategy(Strategy):
    vol_window = 20
    delta_window = 20
    vol_min_threshold = 3
    poc_tolerance = 0.003
    def init(self):
        self.vol_ma = self.I(SMA, self.data.Volume, self.vol_window)
        self.delta_ma = self.I(SMA, self.data.delta, self.delta_window)
    def next(self):
        tolerance_margin = self.data.Close[-1] * self.poc_tolerance
        if self.data.poc[-1] <= self.data.Close[-1] + tolerance_margin and \
            self.data.Volume[-1] > self.vol_ma[-1] * self.vol_min_threshold and \
            self.data.delta_normalized[-1] < 0.5 and \
            self.data.color[-1] == 'red' and \
            self.delta_ma[-1] < 0 and \
            not self.position:
            self.buy(limit= self.data.poc[-1])
            print(f"POC: {self.data.poc[-1]}, Close: {self.data.Close[-1]}, Volume: {self.data.Volume[-1]}, Vol MA: {self.vol_ma[-1]}, Delta Norm: {self.data.delta_normalized[-1]}, Color: {self.data.color[-1]}, Delta MA: {self.delta_ma[-1]}")
        elif self.position:
            if self.data.poc[-1] >= self.data.Close[-1] - tolerance_margin and \
                self.data.Volume[-1] > self.vol_ma[-1] * self.vol_min_threshold and \
                self.data.delta_normalized[-1] > 0.5 and \
                self.data.color[-1] == 'green' and \
                self.delta_ma[-1] > 0:
                print(f"POC: {self.data.poc[-1]}, Close: {self.data.Close[-1]}, Volume: {self.data.Volume[-1]}, Vol MA: {self.vol_ma[-1]}, Delta Norm: {self.data.delta_normalized[-1]}, Color: {self.data.color[-1]}, Delta MA: {self.delta_ma[-1]}")
                self.position.close()

bt = FractionalBacktest(df_ohlc, signal_strategy, cash=100000, commission=0.002, finalize_trades=True, fractional_unit=1e-08)
stat = bt.run()
print(stat)
output_path = os.path.join(path, "signal_strategy.html")
bt.plot(filename=output_path)

""" stats, heatmap = bt.optimize(
        vol_min_threshold = [1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0, 3.5, 4.0, 4.5, 5.0],
        poc_tolerance = [0.0005, 0.001, 0.002, 0.003, 0.004, 0.005],
        maximize = "Return [%]",
        return_heatmap=True,
    )
print(f"Mejor vol_min_threshold: {stats['_strategy'].vol_min_threshold}")
plot_heatmaps(heatmap)
 """