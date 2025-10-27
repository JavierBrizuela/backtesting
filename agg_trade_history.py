from binance_client import BinanceClient
import numpy as np
import pandas as pd
import duckdb
import os

symbol = "BTCUSDT"
date = "2025-10-10"
start_time = pd.Timestamp(date) #formato YYYY-MM-DD hh:mm:ss
end_time = pd.Timestamp(date) + pd.Timedelta(days=1) #formato YYYY-MM-DD hh:mm:ss
folder = f'data/{symbol}/tradebook/'
os.makedirs(folder, exist_ok=True)
file_path = os.path.join(folder, f'agg_trade_history_{date}.parquet')
client = BinanceClient(test_net=False)
db_path = os.path.join(folder, f'{symbol}_trade_history.db')
table = 'agg_trade_history'

def save_trades_to_db(trades, db_path, table):
    df = pd.DataFrame(trade.model_dump() for trade in trades)
    if df.empty:
        print("⚠️ No hay datos para guardar en la base de datos.")
    else:
        df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match', 'additional_properties']
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')# Formato YYYY-MM-DD hh:mm:ss.fff
        df.set_index('timestamp', inplace=True)
        df.drop(columns=['additional_properties'], inplace=True)
        
        con = duckdb.connect(database=db_path)
        con.execute(f'''
            CREATE TABLE IF NOT EXISTS {table} AS 
            SELECT * FROM df LIMIT 0
        ''')
        con.execute(f'''
            INSERT INTO {table} SELECT * FROM df
        ''')
        con.close()
        
def save_trades_to_csv(trades, file_path):
    
    df = pd.DataFrame(trade.model_dump() for trade in trades)
    if df.empty:
        print("⚠️ No hay datos para guardar en CSV.")
    else:
        df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match', 'additional_properties']
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')# Formato YYYY-MM-DD hh:mm:ss.fff
        df.set_index('timestamp', inplace=True)
        df.drop(columns=['additional_properties'], inplace=True)
        if os.path.exists(file_path):
            df.to_csv(file_path, mode='a', header=False, index=True)
        else:
            df.to_csv(file_path, mode='w', header=True, index=True)
            
def save_trades_to_parquet(trades, file_path):
    df = pd.DataFrame(trade.model_dump() for trade in trades)
    if df.empty:
        print("⚠️ No hay datos para guardar en CSV.")
    else:
        df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match', 'additional_properties']
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')# Formato YYYY-MM-DD hh:mm:ss.fff
        df.set_index('timestamp', inplace=True)
        df.drop(columns=['additional_properties'], inplace=True)
        if os.path.exists(file_path):
            existing = pd.read_parquet(file_path)
            df = pd.concat([existing, df])
            
        df.to_parquet(file_path,compression="snappy", index=True)
            
if os.path.exists(file_path):
    df = pd.read_csv(file_path)
    df.index = pd.to_datetime(df['timestamp'])
    last_timestamp = df.index[-1]   
    first_timestamp = df.index[0]
    last_timestamp = last_timestamp.floor('s') #Formato YYYY-MM-DD hh:mm:ss
    first_timestamp = first_timestamp.floor('s') #Formato YYYY-MM-DD hh:mm:ss
    fromId = df['agg_trade_id'].iloc[-1] + 1
    print(f"Data from {first_timestamp} to {last_timestamp} already exists.")

    if start_time >= first_timestamp and end_time <= last_timestamp:
        print("No new data to fetch.")
    
    elif end_time > last_timestamp:
        print("Fetching missing data...")
        response = client.get_aggregate_trades(symbol=symbol, fromId=fromId, start_time=start_time, end_time=end_time)
        print(f"Fetched {len(response)} new trades.")
        save_trades_to_parquet(response, file_path)
else:
    
    response = client.get_aggregate_trades(symbol=symbol, start_time=start_time, end_time=end_time)
    save_trades_to_parquet(response, file_path)
    