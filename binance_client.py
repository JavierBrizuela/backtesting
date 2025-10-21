from datetime import datetime
from pprint import pprint
from binance_sdk_spot.spot import Spot
from binance_common.constants import SPOT_REST_API_PROD_URL, SPOT_REST_API_TESTNET_URL
from binance_common.configuration import ConfigurationRestAPI
import os, time
import logging
import json
from pydantic import BaseModel
import websocket
import pandas as pd
from decorators import log_api_call

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class BinanceClient:
    def __init__(self, test_net=False):
        __api_key = os.getenv("API_KEY") if not test_net else os.getenv("TESTNET_API_KEY")
        __api_secret = os.getenv("SECRET_KEY") if not test_net else os.getenv("TESTNET_SECRET_KEY")
        base_path=SPOT_REST_API_PROD_URL if not test_net else SPOT_REST_API_TESTNET_URL
        __config = ConfigurationRestAPI(api_key=__api_key, api_secret=__api_secret, base_path=base_path)
        
        logging.info(f"Initializing BinanceClient with base_path: {base_path}")
        self.client = Spot(config_rest_api=__config)
    
    def time_to_timestamp(self, start_time, end_time):
        
        if start_time:
            start_timestamp = int(start_time.timestamp() * 1000)

        if not end_time:
            end_timestamp = int(datetime.now().timestamp() * 1000)
        else:
            end_timestamp = int(end_time.timestamp() * 1000)
                
        if end_time and start_time and end_time < start_time:
            raise ValueError("end_time must be greater than or equal to start_time")
        return start_timestamp, end_timestamp
    
    @log_api_call
    def get_order_book(self, symbol, limit=500):
        return self.client.rest_api.depth(symbol=symbol, limit=limit).data()
    
    @log_api_call
    def get_aggregate_trades(self, symbol, fromId=None, start_time=None, end_time=None, limit=1000):
        print(type(start_time))
        start_timestamp = int(start_time.timestamp() * 1000) if start_time else None
        end_timestamp = int(end_time.timestamp() * 1000) if end_time else None
        last_trade_timestamp = 0
        agg_trades = []
        print(f"last_trade_time: {last_trade_timestamp}, start_timestamp: {start_timestamp}, end_timestamp: {end_timestamp}")
    
        params = {
            'symbol': symbol,
            'start_time': start_timestamp,
            'end_time': end_timestamp,
            'limit': limit
        }
        if fromId:
            params['from_id'] = fromId
            del params['start_time']
            del params['end_time']
        
        request_count = 0
        
        while True:
            try:
                print(f"params before request: {params}")
                response = self.client.rest_api.agg_trades(**params)
                status_code = response.status
                rate_limit = response.rate_limits
                response = response.data()
                print(f"status_code: {status_code}, rate_limit: {rate_limit}")
                if not response:
                    print("No more trades to fetch.")
                    break
                
                if len(response) < limit:
                    print("Fetched all available trades.")
                    agg_trades.extend(response)
                    break
                
                last_trade = response[-1].model_dump()
                last_trade_timestamp = last_trade['T']
                params ={
                    'symbol': symbol,
                    'limit': limit,
                    'from_id': last_trade['a'] + 1  # next trade id
                }
                request_count += 1
                print(f"params: {params}")
                print(f"last_trade_time: {last_trade_timestamp}, end_time: {end_timestamp}")
                if last_trade_timestamp >= end_timestamp:
                    filtered = [trade for trade in response if trade['T'] <= end_timestamp]
                    agg_trades.extend(filtered)
                    print("Reached end_time, stopping fetch.")
                    break
                else:
                    agg_trades.extend(response)
                    print(f"Fetched {len(response)} trades, total so far: {len(agg_trades)}")
                
                if request_count % 10 == 0:
                    print(f"Pausa más larga después de {request_count} peticiones...")
                    time.sleep(2)
                else:
                    time.sleep(0.2)
            except KeyboardInterrupt:
                print("Proceso interrumpido por el usuario.")
                break
            except Exception as e:
                logging.error(f"Error fetching aggregate trades: {e}")
                break
        return agg_trades
    
    @log_api_call
    def get_account_info(self):
        return self.client.rest_api.get_account(omit_zero_balances=True).data()
    
    @log_api_call
    def get_symbol_balance(self, symbol):
        account_info = self.get_account_info()
        for balance in account_info.balances:
            if balance.asset == symbol:
                return balance
        return None

    @log_api_call
    def get_ticker_ohlcv(self, symbol, interval, start_time=None, end_time=None, limit=1000):
        
        if start_time:
            start_time, end_time = self.time_to_timestamp(start_time, end_time)
        
        all_data = []
        
        while start_time and end_time and start_time < end_time:
            # Cuando se solicitan mas de 1000 velas, se deben hacer multiples llamadas
            data = self.client.rest_api.klines(symbol=symbol, interval=interval, start_time=start_time, end_time=end_time, limit=limit).data()
            start_time = data[-1][0] + 1 if data else end_time
            all_data.extend(data)

        df = pd.DataFrame(all_data, columns=[
            'open_time', 'Open', 'High', 'Low', 'Close', 'Volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        cols_float = ['Open', 'High', 'Low', 'Close', 'Volume',
              'quote_asset_volume', 'taker_buy_base_asset_volume',
              'taker_buy_quote_asset_volume']

        cols_int = ['number_of_trades', 'ignore']

        df[cols_float] = df[cols_float].astype(float)
        df[cols_int] = df[cols_int].astype(int)
        return df

    @log_api_call
    def market_order(self, symbol, side, quantity=None, quote_order_qty=None):
        return self.client.rest_api.new_order(symbol=symbol, side=side, type="MARKET", quantity=quantity, quote_order_qty=quote_order_qty).data()

    @log_api_call
    def limit_order(self, symbol, side, price, quantity=None, quote_order_qty=None, time_in_force="GTC"):
        return self.client.rest_api.new_order(symbol=symbol, side=side, type="LIMIT", quantity=quantity, price=price, quote_order_qty=quote_order_qty, time_in_force=time_in_force).data()

