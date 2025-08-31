import pandas as pd
import numpy as np
import yfinance as yf
from backtesting import Strategy, Backtest
from backtesting.lib import crossover, FractionalBacktest, plot_heatmaps
import talib
import multiprocessing
import backtesting
import os

def get_data(symbol, interval, period="max", update=False):
    if not os.path.exists("tickers/" + symbol + "_" + interval + ".csv") or update:
        data = yf.download(symbol, period=period, interval=interval)
        print("Datos descargados para:", symbol)
        if isinstance(data.columns, pd.MultiIndex):
            # Crear nombres simples
            new_columns = []
            for col in data.columns:
                if col[0] in ['Open', 'High', 'Low', 'Close', 'Volume']:
                    new_columns.append(col[0])  # Usar Open, High, Low, Close, Volume
                else:
                    new_columns.append(col[1])  # Usar el segundo nivel
            data.columns = new_columns
            
        os.makedirs("tickers", exist_ok=True)
        name_csv = symbol + "_" + interval + ".csv"
        data.to_csv("tickers/" + name_csv, index=True)
    else:
        name_csv = symbol + "_" + interval + ".csv"
        data = pd.read_csv("tickers/" + name_csv, index_col=0, parse_dates=True)
    return data

def SMA(values, n):
   
    return pd.Series(values).rolling(n).mean()

class MyStrategy(Strategy):
    n1 = 33
    n2 = 35
    def init(self):
        self.sma1 = self.I(talib.SMA, self.data.Close, timeperiod=self.n1)
        self.sma2 = self.I(talib.SMA, self.data.Close, timeperiod=self.n2)

    def next(self):
        if crossover(self.sma1, self.sma2) and not self.position.is_long:
            self.position.close()
            self.buy()
        elif crossover(self.sma2, self.sma1) and self.position.is_long:
            self.position.close()
            
if __name__ == "__main__":
    
    # Habilitar multiproceso
    backtesting.Pool = multiprocessing.Pool
    
    symbol = "BTC-USD"
    interval = "1d"
    start = "2023-01-01"
    end = "2025-08-28"
    period= "max" # 4h-1h"730d" , 30m-5m "60d",1m "7d" 
    
    data = get_data(symbol, interval, period, update=False)
    data = data.loc[start:end]
    bt = FractionalBacktest(data, MyStrategy, cash=20000, commission=.002, finalize_trades=True, fractional_unit=1e-08)
    stats = bt.run()
    print(stats)
    bt.plot()
    """ stats, heatmap = bt.optimize(
        n1=range(5, 50,1),
        n2=range(20, 200, 5),
        maximize = "Return [%]",
        return_heatmap=True,
    )
    print("Mejores parámetros encontrados:")
    print(stats["_strategy"])
    print(stats["_trades"])
    plot_heatmaps(heatmap) """