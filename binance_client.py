from pprint import pprint
from binance_sdk_spot.spot import Spot
from binance_common.constants import SPOT_REST_API_PROD_URL, SPOT_REST_API_TESTNET_URL
from binance_common.configuration import ConfigurationRestAPI
import os
import logging

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
    def market_order(self, symbol, side, quantity=None, quote_order_qty=None):
        return self.client.rest_api.new_order(symbol=symbol, side=side, type="MARKET", quantity=quantity, quote_order_qty=quote_order_qty).data()

    @log_api_call
    def limit_order(self, symbol, side, price, quantity=None, quote_order_qty=None, time_in_force="GTC"):
        return self.client.rest_api.new_order(symbol=symbol, side=side, type="LIMIT", quantity=quantity, price=price, quote_order_qty=quote_order_qty, time_in_force=time_in_force).data()
