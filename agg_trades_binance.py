from io import BytesIO
from zipfile import ZipFile
import requests
import pandas as pd

class AggTradesBinanceDownloader:
    """
    Descarga los trades agrupados desde la plataforma de Binance
    y devuelve un data frame con los datos.
    """
    def download_agg_trades_montly(self, symbol, year, month, time_zone=0):
        url = f"https://data.binance.vision/data/spot/monthly/aggTrades/{symbol}/{symbol}-aggTrades-{year}-{month:02d}.zip"
        response = requests.get(url)
        if response.status_code == 200:
            with ZipFile(BytesIO(response.content)) as zip_file:
                csv_name = zip_file.namelist()[0]
                df = pd.read_csv(zip_file.open(csv_name))
                df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match']
                if time_zone != 0:
                    df['timestamp'] = df['timestamp'] + time_zone * 60 * 60 * 1000000
                return df
        else:
            print(f"Failed to download {symbol}-aggTrades-{year}-{month:02d}.zip")
        return None
    
    def download_agg_trades_daily(self, symbol, year, month, day, time_zone=0):
        url = f"https://data.binance.vision/data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{year}-{month:02d}-{day:02d}.zip"
        response = requests.get(url)
        if response.status_code == 200:
            with ZipFile(BytesIO(response.content)) as zip_file:
                csv_name = zip_file.namelist()[0]
                df = pd.read_csv(zip_file.open(csv_name))
                df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match']
                if time_zone != 0:
                    df['timestamp'] = df['timestamp'] + time_zone * 60 * 60 * 1000000
                return df
        else:
            print(f"Failed to download {symbol}-aggTrades-{year}-{month:02d}-{day:02d}.zip")
        return None
    

