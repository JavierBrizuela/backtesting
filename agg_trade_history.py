from binance_client import BinanceClient
import numpy as np
import pandas as pd
import os

symbol = "BTCUSDT"
date = "2025-10-18"
start_time = pd.Timestamp(date)
end_time = pd.Timestamp(date) + pd.Timedelta(days=1)
print(f"data type start_time : {type(start_time)}")
folder = f'data/{symbol}/tradebook/'
os.makedirs(folder, exist_ok=True)
file_path = os.path.join(folder, f'agg_trade_history_{date}.csv')
client = BinanceClient(test_net=False)

if os.path.exists(file_path):
    df = pd.read_csv(file_path, index_col='timestamp', parse_dates=True)
    last_timestamp = df.index[-1]
    
    print(f"type timestamp : {type(last_timestamp)}")
    first_timestamp = df.index[0]
    last_timestamp = last_timestamp.floor('s')
    first_timestamp = first_timestamp.floor('s')
    fromId = df['agg_trade_id'].iloc[-1] + 1
    print(f"Data from {first_timestamp} to {last_timestamp} already exists.")

    if start_time >= first_timestamp and end_time <= last_timestamp:
        print("No new data to fetch.")
    
    elif pd.to_datetime(end_time) > last_timestamp:
        print("Fetching missing data...")
        response = client.get_aggregate_trades(symbol=symbol, fromId=fromId, end_time=end_time)
        
else:
    response = client.get_aggregate_trades(symbol=symbol, start_time=start_time, end_time=end_time)
df = pd.DataFrame(trade.model_dump() for trade in response)
df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match', 'additional_properties']
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df.set_index('timestamp', inplace=True)
df.drop(columns=['additional_properties'], inplace=True)
if os.path.exists(file_path):
    df.to_csv(file_path, mode='a', header=False, index=True)
else:
    df.to_csv(file_path, mode='w', header=True, index=True)