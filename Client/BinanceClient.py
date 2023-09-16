import threading
import time
from datetime import datetime

import pandas as pd
from binance.client import Client
from binance.enums import TIME_IN_FORCE_GTC
from binance.exceptions import BinanceAPIException
from binance.streams import BinanceSocketManager
from events import Events

from MaFin.Utils.Singleton import Singleton


def safe_request(func):
    """
    Decorator for safe binance requests
    """

    def inner(*args, **kwargs):
        for i in range(0, 6):
            try:
                return func(*args, **kwargs)
            except BinanceAPIException as e:
                if i == 5:
                    print(f'Request failed 5 times \n returning empty df')
                    return pd.DataFrame()
                print(f'Request failed \n reason: {str(e)} \n retrying')
                time.sleep(0.5)

    return inner


"""Class for handling requests to Binance and parsing it to suitable format"""


class BinanceClient(metaclass=Singleton):
    def __init__(self, public_key, private_key):
        """
        Safely initializes client from private_file
        If private file is not added to .gitignore, it should be protected with seed
        :param seed: 16 bytes key for AES to decrypt the private key
        """

        self.client = Client(public_key, private_key)
        self.bm = BinanceSocketManager(self.client, user_timeout=60 * 60)
        self.bm_prices = BinanceSocketManager(self.client, user_timeout=60 * 60)
        self.order_filled_event = Events()
        self.balance_changed_event = Events()
        self.klines_prices_sockets = list()

    def get_client(self):
        return self.client

    @safe_request
    def get_symbol_info(self, symbol):
        i = self.client.get_symbol_info(symbol=symbol)
        return self.__parse_symbol_info(i)

    @safe_request
    def get_klines(self, **params):
        """
        :return klines for given symbol in given time frame
        """
        k = self.client.get_klines(**params)
        return self.__parse_klines(k)

    @safe_request
    def get_historical_klines(self, **params):
        """
        :return klines for given symbol in given time frame
        """
        k = self.client.get_historical_klines(**params)
        return self.__parse_klines(k)

    @safe_request
    def get_my_trades(self, **params):
        """
        :return trades for given symbol
        """
        t = self.client.get_my_trades(**params)
        return self.__parse_trades(t)

    @safe_request
    def futures_get_my_trades(self, **params):
        """
        :return trades for given symbol in futures market
        """
        t = self.client.futures_account_trades(**params)
        return self.__parse_trades_futures(t)

    @safe_request
    def get_all_orders(self, **params):
        """
        :return all orders
        """
        o = self.client.get_all_orders(**params)
        return self.__parse_orders(o)

    @safe_request
    def futures_get_all_orders(self, **params):
        """
        :return all orders for futures
        """
        o = self.client.futures_get_all_orders(**params)
        return self.__parse_orders(o)

    @safe_request
    def get_open_orders(self, **params):
        """
        :return open orders
        """
        o = self.client.get_open_orders(**params)
        return self.__parse_orders(o)

    @safe_request
    def futures_get_open_orders(self, **params):
        """
        :return open orders
        """
        o = self.client.futures_get_open_orders(**params)
        return self.__parse_orders(o)

    @safe_request
    def savings_get_lending_product_list(self, **params):
        return self.client.get_liquidity(**params)

    @safe_request
    def get_balance(self, **params):
        """
        :return account balance
        """
        o = self.client.get_asset_balance(**params)
        return self.__parse_balance(o)

    @staticmethod
    def __parse_symbol_info(data):
        df = pd.DataFrame(columns=['symbol', 'status', 'baseAsset', 'baseAssetPrecision', 'quoteAsset',
                                   'quotePrecision', 'icebergAllowed'], data=data, index=[0])

        df['orderTypes'] = [str(data['orderTypes'])]
        df['minPrice'] = [str(data['filters'][0]['minPrice'])]
        df['maxPrice'] = [str(data['filters'][0]['maxPrice'])]
        df['minQty'] = [str(data['filters'][2]['minQty'])]
        df['maxQty'] = [str(data['filters'][2]['maxQty'])]
        df['minNotional'] = [str(data['filters'][3]['minNotional'])]
        return df

    @staticmethod
    def __parse_klines(data):
        df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'vol', 'close_date', 'quote_vol',
                                   'n_trades', 'taker_buy_base_vol', 'taker_buy_quote_vol', 'dummy'], data=data)
        df.set_index('date', inplace=True)
        df.drop('dummy', axis=1, inplace=True)
        return df

    @staticmethod
    def __parse_trades(data):
        df = pd.DataFrame(columns=['id', 'symbol', 'price', 'qty', 'commission', 'commissionAsset', 'time', 'isBuyer',
                                   'isMaker', 'isBestMatch'], data=data)
        df['date'] = df.time
        df.set_index('date', inplace=True)
        return df

    @staticmethod
    def __parse_trades_futures(data):
        df = pd.DataFrame(columns=['id', 'symbol', 'price', 'qty', 'commission', 'commissionAsset', 'time', 'buyer',
                                   'maker', 'isBestMatch'], data=data)
        df['date'] = df.time
        df.set_index('date', inplace=True)
        return df

    @staticmethod
    def __parse_orders(data):
        df = pd.DataFrame(columns=['time', 'symbol', 'orderId', 'clientOrderId', 'price', 'origQty', 'executedQty',
                                   'status', 'timeInForce', 'type', 'side', 'stopPrice', 'icebergQty'], data=data)
        df['date'] = df.time
        df.set_index('orderId', inplace=True)
        return df

    @staticmethod
    def __parse_balance(data):
        df = pd.DataFrame.from_records(columns=['asset', 'free', 'locked'], data=[data])
        return df

    @safe_request
    def create_order(self, symbol, side, quantity, order_type, price=None):
        if order_type == 'LIMIT' and price is None:
            raise Exception('Limit order without price passed!')
        p = self.client.create_order(symbol=symbol, side=side, type=order_type, quantity=quantity, price=price,
                                     timestamp=datetime.timestamp(datetime.now()), timeInForce=TIME_IN_FORCE_GTC)
        return p

    @safe_request
    def cancel_order(self, symbol, order_id):
        r = self.client.cancel_order(symbol=symbol, orderId=order_id)
        return r

    def init_stream(self):
        self.bm = BinanceSocketManager(self.client, user_timeout=60 * 60)
        conn_key = self.bm.start_user_socket(self.__handle_stream_message)
        for c, s in self.klines_prices_sockets:
            self.bm.start_symbol_ticker_socket(c, s)
        self.bm.start()
        self.__keep_stream_alive(conn_key)

    def __kill_stream(self):
        self.bm.close()
        self.bm_prices.close()

    def __keep_stream_alive(self, conn_key):
        keeper = threading.Thread(target=lambda: self.__keep_stream_alive_thread(conn_key))
        keeper.start()

    def __keep_stream_alive_thread(self, conn_key):
        i = 0
        while i < 24:
            self.client.stream_keepalive(conn_key)
            time.sleep(30 * 60)
            i += 1
        self.__kill_stream()
        self.init_stream()

    def __handle_account_change(self, msg):
        pass

    def __handle_order_change(self, msg):
        self.on_order_filled(pair=msg['s'], order_id=msg['i'])

    def __handle_stream_message(self, msg):
        if msg['e'] == 'outboundAccountPosition':
            self.__handle_account_change(msg)
        elif msg['e'] == 'executionReport':
            self.__handle_order_change(msg)
        else:
            print('Undefined message')

    def subscribe_order_filled(self, sub):
        self.order_filled_event.on_change += sub

    def subscribe_kline_price(self, symbol, sub):
        self.klines_prices_sockets.append((symbol, sub))

    def unsubscribe_order_filled(self, sub):
        self.order_filled_event.on_change -= sub

    def on_order_filled(self, **params):
        self.order_filled_event.on_change(**params)

    def on_balance_changed(self, **params):
        self.balance_changed_event.on_change(**params)
