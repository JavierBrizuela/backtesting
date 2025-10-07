import os, time, json
from binance_client import BinanceClient
import numpy as np
import pandas as pd

symbol = "BTCUSDT"
folder = f'data/{symbol}/orderbook/'
os.makedirs(folder, exist_ok=True)
file_path = os.path.join(folder, 'order_book.csv')
buffer = []
max_buffer_len = 100
client = BinanceClient(test_net=False)

def group_orderbook(orders, band_size):
    orders = np.array(orders, dtype=float)
    price = orders[:, 0]
    volume = orders[:, 1]
    bands = price // band_size * band_size
    bands_unique = np.unique(bands)
    
    result = []
    for band in bands_unique:
        mask = bands == band
        total_volume = volume[mask].sum()
        result.append([band, total_volume])

    return np.array(result)

def save_buffer_to_csv(buffer, file_path):
    """Guarda el buffer al CSV"""
    if not buffer:
        return
    
    df = pd.DataFrame(buffer, columns=[
        'timestamp', 'last_update_id', 'side', 'band_price', 'total_volume'
    ])
    
    df.to_csv(file_path, mode='a', header=False, index=False)
    rows_saved = len(buffer)
    buffer.clear()
    
    return rows_saved

""" with open(file_path, 'r') as f:
    data = json.load(f)
    print(f"Order book snapshot loaded from {file_path}")
    print(data['asks'])
   
    print(data['bids'])
   
    banda = 100
    result_asks = group_orderbook(data['asks'], banda)
    print(result_asks)
    result_bids = group_orderbook(data['bids'], banda)
    print(result_bids) """
if __name__ == "__main__":
    if not os.path.exists(file_path):
        pd.DataFrame(columns=[
                    'timestamp','last_update_id', 'side', 'band_price', 
                    'total_volume'
                ]).to_csv(file_path, index=False)
        print(f"✓ Archivo CSV creado: {file_path}")
        
    while True:

        depth = client.get_order_book(symbol=symbol, limit=500)
        snapshot = depth.model_dump()
        bids = group_orderbook(snapshot['bids'], 100)
        asks = group_orderbook(snapshot['asks'], 100)
        last_update_id = snapshot['last_update_id']
        buffer.extend([(last_update_id, time.time(), "bids", bids[i][0], bids[i][1]) for i in range(len(bids))])
        buffer.extend([(last_update_id, time.time(), "asks", asks[i][0], asks[i][1]) for i in range(len(asks))])
        if len(buffer) >= max_buffer_len:
            rows = save_buffer_to_csv(buffer, file_path)
            print(f"Order book snapshot saved to {file_path}")
        time.sleep(2.0)