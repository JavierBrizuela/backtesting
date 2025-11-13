from agg_trades_binance import AggTradesBinanceDownloader
from agg_trades_db import AggTradeDB
import pandas as pd
import os
db_path = 'data/BTCUSDT/tradebook/trade_history.db'
table = 'agg_trades'
simbol = 'BTCUSDT'

agg_trades_df = AggTradesBinanceDownloader()
if os.path.exists(db_path):
    agg_trades_DB = AggTradeDB(db_path)
    timestamp = agg_trades_DB.con.execute("SELECT MAX(timestamp) FROM {table}").fetch_df()
    print(f"Timestamp obtenido de la base de datos: {timestamp.head()}")
    if timestamp is not None:
        start_date = pd.to_datetime(timestamp, unit='us')
        print(f"Último timestamp en la base de datos: {start_date}")
else:
    today = pd.Timestamp.now()
    year = today.year
    month = today.month
    day = today.day
    print(f"La base de datos no existe. Se creara y descargarán todos los datos disponibles. hasta la fecha: {today}")
    
    agg_trades_DB = AggTradeDB(db_path)
    for m in range(1, month):
        df = agg_trades_df.download_agg_trades_montly(simbol, year, m, -3)
        agg_trades_DB.save_df_to_db(df, table)
    for d in range(1, day):
        df = agg_trades_df.download_agg_trades_daily(simbol, year, month, d, -3)
        agg_trades_DB.save_df_to_db(df, table)
    agg_trades_DB.close_connection()
