import pandas as pd
import numpy as np
import yfinance as yf
from backtesting import Strategy, Backtest
from backtesting.lib import crossover
import talib

symbol = "BTC-USD"
interval = "1d"
start = "2025-01-01"
end = "2025-08-28"
data = yf.download(symbol, start=start, end=end, interval=interval)

if isinstance(data.columns, pd.MultiIndex):
    # Crear nombres simples
    new_columns = []
    for col in data.columns:
        if col[0] in ['Open', 'High', 'Low', 'Close']:
            new_columns.append(col[0])  # Usar Open, High, Low, Close
        elif col[0] == 'Volume':
            new_columns.append('Volume')
        else:
            new_columns.append(col[1])  # Usar el segundo nivel
    
    data.columns = new_columns

class MyStrategy(Strategy):
    n1 = 10
    n2 = 20
    def init(self):
        self.sma1 = self.I(talib.SMA, self.data['Close'], timeperiod=self.n1)
        self.sma2 = self.I(talib.SMA, self.data['Close'], timeperiod=self.n2)

    def next(self):
        if crossover(self.sma1, self.sma2):
            self.position.close()
            self.buy()
        elif crossover(self.sma2, self.sma1):
            self.position.close()
            self.sell()

bt = Backtest(data, MyStrategy, cash=10000, commission=.002)
stats = bt.run()
print(stats)
bt.plot()
stats = bt.optimize(
    n1=(5, 50, 5),
    n2=(20, 200, 5),
    maximize = "Return [%]"
)
print(stats._strategy)
