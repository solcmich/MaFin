from MaFin.Client.BinanceClient import BinanceClient
from MaFin.Orders.SmartOrder import SmartOrder
from MaFin.Data.BinanceDataProvider import BinanceDataProvider
from MaFin.Utils.Singleton import Singleton


def safe_order(func):
    """
    Decorator for safe order placements
    todo: check min quantity, min notional etc.
    """

    def inner(*args, **kwargs):
        return func(*args, **kwargs)
    return inner


class OrdersManager(metaclass=Singleton):
    def __init__(self, client: BinanceClient, storage_manager: BinanceDataProvider):
        self.client = client
        self.storage_manager = storage_manager
        self.smart_orders = dict()

    def create_order_limit_buy(self, symbol, quantity, price):
        return self.__create_order(symbol=symbol, side='BUY', quantity=quantity, order_type='LIMIT', price=price)

    def create_order_limit_sell(self, symbol, quantity, price):
        return self.__create_order(symbol=symbol, side='SELL', quantity=quantity, order_type='LIMIT', price=price)

    def create_order_market_sell(self, symbol, quantity):
        return self.__create_order(symbol=symbol, side='SELL', quantity=quantity, order_type='MARKET')

    def create_order_market_buy(self, symbol, quantity):
        return self.__create_order(symbol=symbol, side='BUY', quantity=quantity, order_type='MARKET')

    @safe_order
    def __create_order(self, **params):
        response = self.client.create_order(symbol=params['symbol'], side=params['side'],
                                            quantity=str(params['quantity']), order_type=params['order_type'],
                                            price=params['price'])
        return response['orderId']

    def get_order(self, order_id):
        orders = self.storage_manager.get_spot_orders()
        val = orders[orders['orderId'] == order_id]
        return val

    def get_all_open_orders(self):
        self.storage_manager.get_spot_open_orders()

    def cancel_order(self, symbol, order_id):
        return self.client.cancel_order(symbol, order_id)

    def cancel_all_open_orders(self):
        all_orders = self.storage_manager.get_spot_open_orders()
        for idx, row in all_orders.iterrows():
            self.cancel_order(int(row['orderId']), str(row['symbol']))
