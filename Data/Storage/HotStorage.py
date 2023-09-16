import threading

import pandas as pd

"""Base storage class for storing any type of data"""


class __BaseHotStorage(threading.Thread):
    def __init__(self):
        """
        Initializes storage for candle data in multi-threading manner
        """
        threading.Thread.__init__(self)
        self.data = None

    def save(self, **params):
        """
        Overrides data in storage
        """
        self.data = params['data']

    def get(self, **params) -> pd.DataFrame:
        """
        :return Data from the storage
        """
        if self.data is not None:
            return self.data
        else:
            return pd.DataFrame()
