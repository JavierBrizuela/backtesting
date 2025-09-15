import pandas as pd
import numpy as np
import yfinance as yf
from backtesting import Strategy, Backtest
from backtesting.lib import crossover, FractionalBacktest, plot_heatmaps
import talib
import multiprocessing
import backtesting
import os
import matplotlib.pyplot as plt

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

def VolumeProfile(data, bins=50):
    # Calcular el perfil de volumen
    price_bins = np.linspace(data['Low'].min(), data['High'].max(), bins)
    hist, edges = np.histogram(data['Close'], bins=price_bins, weights=data['Volume'])
    return hist, edges

def squeeze_momentum(df, length=20, mult=2.0, lengthKC=20, multKC=1.5):
    high = df['High']
    low = df['Low']
    close = df['Close']
    # Calcular Bollinger Bands
    upperBB, basis, lowerBB = talib.BBANDS(close, timeperiod=length, nbdevup=mult, nbdevdn=mult, matype=0)
    # Calcular Keltner Channels
    atr = talib.ATR(high, low, close, timeperiod=lengthKC)
    ma = talib.SMA(close, timeperiod=lengthKC)
    upperKC = ma + atr * multKC
    lowerKC = ma - atr * multKC
    # Determinar si el mercado está en "squeeze"
    squeeze_on = (lowerBB > lowerKC) & (upperBB < upperKC)
    squeeze_off = (lowerBB < lowerKC) & (upperBB > upperKC)
    no_squeeze = ~(squeeze_on | squeeze_off)
    # Calcular el Momentum
    highest_high = pd.Series(high).rolling(lengthKC).max()
    lowest_low = pd.Series(low).rolling(lengthKC).min()
    avg1 = (highest_high + lowest_low) / 2
    avg2 = pd.Series(close).rolling(lengthKC).mean()
    input_series = pd.Series(close) - (avg1 + avg2) / 2
    # Función para calcular la regresión lineal
    def linreg(series, length, offset=0):
        x = np.arange(length)
        def f(y):
            if np.isnan(y).any():
                return np.nan
            coeffs = np.polyfit(x, y, 1)
            return coeffs[1] + coeffs[0] * (length - 1 - offset)
        return series.rolling(length).apply(f, raw=True)

    val = linreg(input_series, lengthKC, 0)

    return val.values
    
class MyStrategy(Strategy):
    n1 = 10
    n2 = 55
    adx_threshold = 19
    def init(self):
        self.ema1 = self.I(talib.EMA, self.data.Close, timeperiod=self.n1)
        self.ema2 = self.I(talib.EMA, self.data.Close, timeperiod=self.n2)
        self.adx = self.I(talib.ADX, self.data.High, self.data.Low, self.data.Close, timeperiod=14)
        self.sqz = self.I(squeeze_momentum, self.data)
        self.bbands = self.I(talib.BBANDS, self.data.Close, timeperiod=20, nbdevup=2.0, nbdevdn=2.0, matype=0)
    def next(self):
        if crossover(self.ema1, self.ema2) and not self.position.is_long and self.adx[-1] > self.adx_threshold:
            self.position.close()
            self.buy()
        elif crossover(self.ema2, self.ema1) and self.position.is_long:
            self.position.close()
            
if __name__ == "__main__":
    
    # Habilitar multiproceso
    backtesting.Pool = multiprocessing.Pool
    
    symbol = "BTC-USD"
    interval = "4h"  # 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    start = "2023-01-01"
    end = "2025-08-28"
    period= "730d"#1d"max" 4h-1h"730d" , 30m-5m "60d",1m "7d" 
    
    data = get_data(symbol, interval, period, update=False)
    data = data.loc[start:end]
    hist, edges = VolumeProfile(data, bins=100)
    """ # Graficar precios y perfil de volumen en subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12,6), gridspec_kw={"width_ratios":[3,1]})

    # Cierre BTC
    ax1.plot(data.index, data["Close"], color="black")
    ax1.set_title("BTC-USD")

    # Perfil de volumen
    ax2.barh(edges[:-1], hist, height=np.diff(edges), alpha=0.6, color="blue")
    ax2.set_title("Perfil de Volumen")
    ax2.set_xlabel("Volumen")
    ax2.set_ylabel("Precio")

    plt.tight_layout()
    plt.show() """
    bt = FractionalBacktest(data, MyStrategy, cash=20000, commission=.002, finalize_trades=True, fractional_unit=1e-08)
    stats = bt.run()
    print(stats)
    bt.plot()
    """stats, heatmap = bt.optimize(
        n1=range(5, 50,1),
        n2=range(20, 200, 5),
        adx_threshold=range(10, 50, 1),
        maximize = "Return [%]",
        return_heatmap=True,
    ) 
    print("Mejores parámetros encontrados:")
    print(stats["_strategy"])
    print(stats["_trades"])
    plot_heatmaps(heatmap) """