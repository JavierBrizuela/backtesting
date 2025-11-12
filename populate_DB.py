from agg_trades_binance import AggTradesBinanceDownloader
from agg_trades_db import AggTradeDB
import pandas as pd

db_path = 'data/BTCUSDT/tradebook/trade_history.db'
table = 'agg_trades'
simbol = 'BTCUSDT'

agg_trades_df = AggTradesBinanceDownloader()
agg_trades_DB = AggTradeDB(db_path)

today = pd.Timestamp.now()
year = today.year
month = today.month
day = today.day

for m in range(1, month):
    df = agg_trades_df.download_agg_trades_montly(simbol, year, m, -3)
    agg_trades_DB.save_df_to_db(df, table)
for d in range(1, day):
    df = agg_trades_df.download_agg_trades_daily(simbol, year, month, d, -3)
    agg_trades_DB.save_df_to_db(df, table)

agg_trades_DB.close_connection()