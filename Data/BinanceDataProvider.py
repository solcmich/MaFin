import json
import os
import threading

import pandas as pd

from MaFin.Data.Storage.CandlesHotStorage import CandlesHotStorage
from MaFin.Data.Storage.SelfUpdatedStorage import SelfUpdatedStorage
from MaFin.Utils.Singleton import Singleton
from MaFin.Utils.Utils import get_seconds_to_kline_close

""" Master class for storage """
"""
Basically runs all the storages needed and provides the option to perform "hot update"
hot update is triggered by some event from outside in order to maintain maximum timeliness
"""


class BinanceDataProvider(metaclass=Singleton):
    def __init__(self, root_storage_path, client):
        """
        Initializes new storage manager with help of configuration.
        Should only be one instance in running app
        """
        with open('../config.json') as config_file:
            config = json.load(config_file)

        self.default_pairs = config["binance"]["default_pairs"]
        self.default_futures_pairs = config["binance"]["default_futures_pairs"]
        self.default_time_frames = config["binance"]["default_time_frames"]
        self.default_symbols = config['binance']['default_symbols']
        self.client = client
        self.hot_events = dict()

        self.pairs_storage = dict()
        self.spot_trades_storage = dict()
        self.spot_orders_storage = dict()
        self.spot_open_orders_storage = dict()
        self.futures_trades_storage = dict()
        self.futures_orders_storage = dict()
        self.balance_storage = dict()
        self.candles_hot_storage = dict()

        self.client.subscribe_order_filled(self.on_order_change)
        self.root_storage_path = root_storage_path

    """ Public api """

    def run(self):
        """
        Isn't it obvious?
        """
        self.__run_pairs_storage()
        self.__run_spot_trades_storage()
        self.__run_spot_orders_storage()
        self.__run_spot_open_orders_storage()
        self.__run_futures_trades_storage()
        self.__run_futures_orders_storage()
        self.__run_balance_storage()
        self.__run_hot_candles_storage()
        self.__run_trading_rules_storage()
        # self.client.init_stream()

    def invoke_hot_event(self, symbol, event_type):
        """
        Entry point for hot event execution
        """
        self.hot_events[(event_type, symbol)].set()

    def get_klines(self, pair=None):
        """
        :return klines for given pair, or for every pair when pair is not specified
        """
        return self._get(self.pairs_storage, pair)

    def get_balance(self, symbol=None):
        """
        :return balance for given symbol, or for every pair when pair is not specified
        """
        return self._get(self.balance_storage, symbol)

    def get_spot_trades(self, pair=None):
        """
        :return spot trades for given pair, or for every pair when pair is not specified
        """
        return self._get(self.spot_trades_storage, pair)

    def get_spot_orders(self, pair=None):
        """
        :return spot orders for given pair, or for every pair when pair is not specified
        """
        return self._get(self.spot_orders_storage, pair)

    def get_spot_open_orders(self, pair=None):
        """
        :return spot open orders for given pair, or for every pair when pair is not specified
        """
        return self._get(self.spot_open_orders_storage, pair)

    def get_futures_trades(self, pair=None):
        """
        :return futures trades for given pair, or for every pair when pair is not specified
        """
        return self._get(self.futures_trades_storage, pair)

    def get_futures_orders(self, pair=None):
        """
        :return futures orders for given pair, or for every pair when pair is not specified
        """
        return self._get(self.futures_orders_storage, pair)

    def get_current_prices(self, pair=None):
        """
        :return current prices for pairs
        """
        return self._get(self.candles_hot_storage, pair)

    """ Private methods """

    @staticmethod
    def _get(storage, pair):
        if pair is None:
            return pd.concat([x.get() for x in storage.values()])
        return storage[pair].get()

    def __run_pairs_storage(self):
        for p in self.default_pairs:
            for tf in self.default_time_frames:
                e = threading.Event()
                dirname = os.path.join(self.root_storage_path, f'Spot/Pairs/{p}/{tf}/data.csv')
                s = SelfUpdatedStorage(dirname, self.client.get_klines, e,
                                       lambda: get_seconds_to_kline_close(tf), symbol=p, interval=tf)
                # todo: Handle initial load
                # s.initial_load(self.client.get_historical_klines, symbol=p, interval=tf, start_str='1600000000000')
                s.start()

    def __run_spot_trades_storage(self):
        for p in self.default_pairs:
            e = threading.Event()
            self.hot_events[("spot_trades", str(p))] = e
            dirname = os.path.join(self.root_storage_path, f'Spot/Trades/{p}/data.csv')
            s = SelfUpdatedStorage(dirname, self.client.get_my_trades, e, symbol=p, startTime=1588080065000)
            s.start()
            self.spot_trades_storage[p] = s

    def __run_spot_orders_storage(self):
        for p in self.default_pairs:
            e = threading.Event()
            self.hot_events[("spot_orders", str(p))] = e
            dirname = os.path.join(self.root_storage_path, f'Spot/Orders/{p}/data.csv')
            s = SelfUpdatedStorage(dirname, self.client.get_all_orders, e, symbol=p)
            s.start()
            self.spot_orders_storage[p] = s

    def __run_spot_open_orders_storage(self):
        for p in self.default_pairs:
            e = threading.Event()
            self.hot_events[("spot_open_orders", str(p))] = e
            dirname = os.path.join(self.root_storage_path, f'Spot/OpenOrders/{p}/data.csv')
            s = SelfUpdatedStorage(dirname, self.client.get_open_orders, e, symbol=p)
            s.start()
            self.spot_open_orders_storage[p] = s

    def __run_futures_trades_storage(self):
        for p in self.default_futures_pairs:
            e = threading.Event()
            self.hot_events[("futures_trades", str(p))] = e
            dirname = os.path.join(self.root_storage_path, f'Futures/Trades/{p}/data.csv')
            s = SelfUpdatedStorage(dirname, self.client.futures_get_my_trades, e, symbol=p)
            s.start()
            self.futures_trades_storage[p] = s

    def __run_futures_orders_storage(self):
        for p in self.default_futures_pairs:
            e = threading.Event()
            self.hot_events[("futures_orders", str(p))] = e
            dirname = os.path.join(self.root_storage_path, f'Futures/Orders/{p}/data.csv')
            s = SelfUpdatedStorage(dirname, self.client.futures_get_all_orders, e, symbol=p)
            s.start()
            self.futures_orders_storage[p] = s

    def __run_balance_storage(self):
        e = threading.Event()
        self.hot_events["balance"] = e
        for p in self.default_symbols:
            dirname = os.path.join(self.root_storage_path, f'Balance/{p}/data.csv')
            s = SelfUpdatedStorage(dirname, self.client.get_balance, e, asset=p)
            s.start()
            self.balance_storage[p] = s

    def __run_hot_candles_storage(self):
        for p in self.default_pairs:
            s = CandlesHotStorage(p)
            self.candles_hot_storage[p] = s
            self.client.subscribe_kline_price(p, s.save)

    def __run_trading_rules_storage(self):
        e = threading.Event()
        for p in self.default_pairs:
            dirname = os.path.join(self.root_storage_path, f'Rules/{p}/data.csv')
            s = SelfUpdatedStorage(dirname, self.client.get_symbol_info, e, symbol=p)
            s.start()

    def on_order_change(self, **params):
        print(f'Order status changed for {params["pair"]}')
        self.hot_events[('spot_orders', params['pair'])].set()
        self.hot_events[('spot_open_orders', params['pair'])].set()
        self.hot_events[('spot_trades', params['pair'])].set()
        self.hot_events['balance'].set()
