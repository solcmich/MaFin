import threading
import time
import pandas as pd

from MaFin.Data.Storage.ColdStorage import ColdStorage


class SmartOrder(threading.Thread):
    def __init__(self, orders_manager, symbol, side, quantity, price_predicate, min_delta=0.05):
        super().__init__()
        self.price_predicate = price_predicate
        self.current_price = price_predicate()
        self.quantity = quantity
        self.min_delta = min_delta
        self.orders_manager = orders_manager
        self.side = side
        self.symbol = symbol
        self.order_id = self.__create_order()
        self.filled = False

        self.hot_event = threading.Event()
        self.storage = ColdStorage('path')

    def run(self):
        while not self.filled:
            if self.__get_change() >= self.min_delta:
                self.orders_manager.cancel_order(self.symbol, self.order_id)
                self.order_id = self.__create_order()
                self.current_price = self.price_predicate()
            time.sleep(10)

        self.storage.dispose()

    def populate_data(self, **params):
        df = pd.DataFrame()
        df['orderId'] = [self.order_id]
        df['currentPrice'] = [self.current_price]
        return df

    def on_order_filled(self):
        self.filled = True

    def __get_change(self):
        current = self.price_predicate()
        return (abs(current - self.current_price) / self.current_price) * 100.0

    def __create_order(self):
        return self.orders_manager.create_order_limit_sell(self.symbol, self.quantity, self.current_price) \
            if self.side == 'SELL' else self.orders_manager.create_order_limit_buy(self.symbol, self.quantity,
                                                                                   self.current_price)
