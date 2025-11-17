from agg_trades_binance import AggTradesBinanceDownloader
from agg_trades_db import AggTradeDB
import pandas as pd

db_path = 'data/BTCUSDT/tradebook/agg_trades.db'
table = 'agg_trades'
simbol = 'BTCUSDT'

def agg_trades_monthly(start_year, start_month, end_year, end_month, simbol):
    for year in range(start_year, end_year + 1):
        for month in range(start_month, end_month):
            df = agg_trades_df.download_agg_trades_montly(simbol, year, month, -3)
            agg_trades_DB.save_df_to_db(df, table)

def agg_trades_daily(year, month, start_day, end_day, simbol): 
    for day in range(start_day, end_day):
        df = agg_trades_df.download_agg_trades_daily(simbol, year, month, day, time_zone)
        agg_trades_DB.save_df_to_db(df, table)

time_zone = -3  # GMT-3
today = pd.Timestamp.now()
end_year = today.year
end_month = today.month
end_day = today.day

agg_trades_df = AggTradesBinanceDownloader()
agg_trades_DB = AggTradeDB(db_path)
tables = agg_trades_DB.con.execute("SHOW TABLES;").fetchdf()

if table in tables['name'].values:
    timestamp = agg_trades_DB.con.execute(f"SELECT MAX(timestamp) FROM {table}").fetch_df().iloc[0,0]
    last_date = pd.Timestamp(timestamp, unit='us')
    print(f"Último timestamp en la base de datos: {last_date}")
    start_year = last_date.year
    start_month = last_date.month
    start_day = last_date.day + 1 # Comenzar desde el día siguiente
    if start_year < end_year or (start_year == end_year and start_month < end_month):
        print(f"Actualizando datos mensuales desde {start_year}-{start_month} hasta {end_year}-{end_month-1}")
        agg_trades_monthly(start_year, start_month, end_year, end_month, simbol)
        start_day = 1  # Reiniciar el día para la descarga diaria
    if start_year == end_year and start_month == end_month and start_day < end_day:
        print(f"Actualizando datos diarios desde {start_year}-{start_month}-{start_day} hasta {end_year}-{end_month}-{end_day-1}")
        agg_trades_daily(end_year, end_month, start_day, end_day, simbol)
else:  
    print(f"La base de datos no existe. Se creara y descargarán todos los datos disponibles. hasta la fecha: {end_year}-{end_month}-{end_day-1}")
    start_year = 2025
    start_month = 1
    start_day = 1
    agg_trades_monthly(start_year, start_month, end_year, end_month, simbol)
    agg_trades_daily(end_year, end_month, 1, end_day, simbol)
interval = '1 minutes'
start = f'{start_year}-{start_month}-{start_day}'
end = f'{end_year}-{end_month}-{end_day}'
agg_trades_DB.ohlc_table(interval, start, end)

agg_trades_DB.close_connection()
    