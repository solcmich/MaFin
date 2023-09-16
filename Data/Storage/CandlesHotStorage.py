import pandas as pd

from MaFin.Data.Storage.HotStorage import __BaseHotStorage


class CandlesHotStorage(__BaseHotStorage):
    def __init__(self, symbol):
        super().__init__()
        self.symbol = symbol
        self.data = None

    def save(self, val):
        """
        Overrides data in storage
        """
        df = pd.DataFrame()
        df['symbol'] = [val['s']]
        df['price'] = [val['c']]
        self.data = df

    def get(self):
        """
        :return Data from the storage
        """
        if self.data is not None:
            return self.data
        else:
            return pd.DataFrame()
