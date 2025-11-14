from agg_trades_binance import AggTradesBinanceDownloader
from agg_trades_db import AggTradeDB
import pandas as pd
import os
db_path = 'data/BTCUSDT/tradebook/agg_trades.db'
table = 'agg_trades'
simbol = 'BTCUSDT'

agg_trades_df = AggTradesBinanceDownloader()
agg_trades_DB = AggTradeDB(db_path)
tables = agg_trades_DB.con.execute("SHOW TABLES;").fetchdf()

today = pd.Timestamp.now()
end_year = today.year
end_month = today.month
end_day = today.day

if table in tables['name'].values:
    timestamp = agg_trades_DB.con.execute(f"SELECT MAX(timestamp) FROM {table}").fetch_df().iloc[0,0]
    last_date = pd.Timestamp(timestamp, unit='us')
    print(f"Último timestamp en la base de datos: {last_date}")
    start_year = last_date.year
    start_month = last_date.month
    start_day = last_date.day
else:  
    print(f"La base de datos no existe. Se creara y descargarán todos los datos disponibles. hasta la fecha: {today}")
    start_year = 2025
    start_month = 1
    start_day = 1

for m in range(1, end_month):
    df = agg_trades_df.download_agg_trades_montly(simbol, end_year, m, -3)
    agg_trades_DB.save_df_to_db(df, table)
for d in range(1, end_day):
    df = agg_trades_df.download_agg_trades_daily(simbol, end_year, end_month, d, -3)
    agg_trades_DB.save_df_to_db(df, table)

agg_trades_DB.close_connection()
    