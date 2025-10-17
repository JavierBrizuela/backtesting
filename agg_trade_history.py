from binance_client import BinanceClient
import numpy as np
import pandas as pd
import os

symbol = "BTCUSDT"
folder = f'data/{symbol}/tradebook/'
os.makedirs(folder, exist_ok=True)
file_path = os.path.join(folder, 'agg_trade_history.csv')

client = BinanceClient(test_net=False)
response = client.get_aggregate_trades(symbol=symbol, start_time="2025-10-01")
df = pd.DataFrame(trade.model_dump() for trade in response)
df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match', 'additional_properties']
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df.drop(columns=['additional_properties'], inplace=True)
df.to_csv(file_path, index='timestamp')