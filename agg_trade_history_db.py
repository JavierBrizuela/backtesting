from io import BytesIO
from zipfile import ZipFile
import requests
from binance_client import BinanceClient
import pandas as pd
import duckdb
import os

symbol = "BTCUSDT"
date = "2025-10-10"
start_time = pd.Timestamp(date) #formato YYYY-MM-DD hh:mm:ss
end_time = pd.Timestamp(date) + pd.Timedelta(days=1) #formato YYYY-MM-DD hh:mm:ss
folder = f'data/{symbol}/tradebook/'
os.makedirs(folder, exist_ok=True)
client = BinanceClient(test_net=False)
db_path = os.path.join(folder, f'trade_history.db')
table = 'agg_trade_history'

def get_agg_trades_monthly(symbol, year, month):
    url = f"https://data.binance.vision/data/spot/monthly/aggTrades/{symbol}/{symbol}-aggTrades-{year}-{month:02d}.zip"
    response = requests.get(url)
    if response.status_code == 200:
       with ZipFile(BytesIO(response.content)) as zip_file:
            csv_name = zip_file.namelist()[0]
            df = pd.read_csv(zip_file.open(csv_name))
            df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match']
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')# Formato YYYY-MM-DD hh:mm:ss.fff
            return df
    else:
        print(f"Failed to download {symbol}-aggTrades-{year}-{month:02d}.zip")
        return None

def agg_trades_from_response_to_df(trades):
    
    df = pd.DataFrame(trade.model_dump() for trade in trades)
    if df.empty:
        print("⚠️ No hay datos para para crear el dataframe.")
        return None
    else:
        df.columns = ['agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'timestamp', 'is_buyer_maker', 'is_best_match', 'additional_properties']
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')# Formato YYYY-MM-DD hh:mm:ss.fff
        df.drop(columns=['additional_properties'], inplace=True)
        return df

def save_agg_trades_to_db(df, db_path, table):
    if df.empty:
        print("⚠️ No hay datos en el dataframe para crear la tabla.")
        return None
    
    con = duckdb.connect(database=db_path)
    con.execute(f'''
        CREATE TABLE IF NOT EXISTS {table} (
            agg_trade_id BIGINT PRIMARY KEY,
            price DOUBLE,
            quantity DOUBLE,
            first_trade_id BIGINT,
            last_trade_id BIGINT,
            timestamp TIMESTAMP,
            is_buyer_maker BOOLEAN,
            is_best_match BOOLEAN
        )
    ''')
    count_before = con.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()[0]
    con.execute(f'''
        INSERT OR IGNORE INTO {table} SELECT * FROM df
    ''')
    count_after = con.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()[0]
    print(f"Registros antes: {count_before}, después: {count_after}, añadidos: {count_after - count_before}")
    con.close()
        
def create_candlesticks_table(db_path, table, interval, candle_table=None):
    if candle_table is None:
        candle_table = f'candles_{interval.replace(" ", "_")}'
    con = duckdb.connect(database=db_path)
    con.execute(f'''
        CREATE TABLE IF NOT EXISTS {candle_table} AS 
        SELECT 
            time_bucket('{interval}', timestamp) AS open_time,
            FIRST(price) AS open,
            MAX(price) AS high,
            MIN(price) AS low,
            LAST(price) AS close, 
            SUM(quantity) AS volume,
            COUNT(*) AS trade_count
        FROM {table} 
        GROUP BY open_time
        ORDER BY open_time ASC
    ''')
    con.close()
    
def get_agg_trades_from_api(symbol, start_time, end_time):
    if os.path.exists(db_path):
        con = duckdb.connect(database=db_path)
        query = f"SELECT MAX(timestamp) AS last_timestamp, MIN(timestamp) AS first_timestamp, MAX(agg_trade_id) AS fromId FROM {table}"
        result = con.execute(query).fetchone()
        con.close()
        last_timestamp = result['last_timestamp'].floor('s') #Formato YYYY-MM-DD hh:mm:ss
        first_timestamp = result['first_timestamp'].floor('s') #Formato YYYY-MM-DD hh:mm:ss
        fromId = result['fromId'] + 1
        print(f"Data from {first_timestamp} to {last_timestamp} already exists.")

        if start_time >= first_timestamp and end_time <= last_timestamp:
            print("No new data to fetch.")
        
        elif end_time > last_timestamp:
            print("Fetching missing data...")
            response = client.get_aggregate_trades(symbol=symbol, fromId=fromId, start_time=start_time, end_time=end_time)
            print(f"Fetched {len(response)} new trades.")
            df =agg_trades_from_response_to_df(response)
            save_agg_trades_to_db(df, db_path, table)
            print(f"{len(response)} trades saved to the database.")
    else:
        
        response = client.get_aggregate_trades(symbol=symbol, start_time=start_time, end_time=end_time)
        df = agg_trades_from_response_to_df(response)
        save_agg_trades_to_db(df, db_path, table)
        print(f"Database created and {len(response)} trades saved.")
year = 2025
month = 9
interval = '4 hours'

df = get_agg_trades_monthly(symbol, year, month)

if df is not None:
    save_agg_trades_to_db(df, db_path, table)
    print(f"{len(df)} trades from {month} - {year} saved to the database.")

#create_candlesticks_table(db_path, table, interval)