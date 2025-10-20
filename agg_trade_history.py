from binance_client import BinanceClient
import numpy as np
import pandas as pd
import os

symbol = "BTCUSDT"
start_time="2025-10-18 00:00:00"
end_time= pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

folder = f'data/{symbol}/tradebook/'
os.makedirs(folder, exist_ok=True)
file_path = os.path.join(folder, f'agg_trade_history_{start_time[0:10]}.csv')
client = BinanceClient(test_net=False)

if os.path.exists(file_path):
    df = pd.read_csv(
        file_path, 
        index_col='timestamp',
        parse_dates=['timestamp']
        )
    
    last_timestamp = df.index[-1]
    first_timestamp = df.index[0]
    last_timestamp = last_timestamp.floor('s')
    first_timestamp = first_timestamp.floor('s')
    print(f"Data from {first_timestamp} to {last_timestamp} already exists.")

    if pd.to_datetime(start_time) >= first_timestamp and (pd.to_datetime(end_time) <= last_timestamp):
        print("No new data to fetch.")
    
    elif pd.to_datetime(end_time) > last_timestamp:
        print("Fetching missing data...")
        response = client.get_aggregate_trades(symbol=symbol, start_time=last_timestamp, end_time=end_time)
        
else:
    response = client.get_aggregate_trades(symbol=symbol, start_time=start_time)
df = pd.DataFrame(trade.model_dump() for trade in response)
df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match', 'additional_properties']
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df.set_index('timestamp', inplace=True)
df.drop(columns=['additional_properties'], inplace=True)
df.to_csv(file_path, mode='a')