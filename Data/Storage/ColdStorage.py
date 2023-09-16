import abc
import os
import threading

import pandas as pd
from pandas.errors import EmptyDataError

"""Base storage class for storing any type of data"""


class ColdStorage(threading.Thread):
    def __init__(self, storage_path):
        """
        Initializes storage for candle data in multi-threading manner
        :param storage_path Path to .csv storage file
        """
        threading.Thread.__init__(self)
        self.lock = threading.RLock()
        self.storage = storage_path
        self.__init_storage()

    def initial_load(self, endpoint, **params):
        self.save(endpoint(**params))

    def append(self, data: pd.DataFrame, sync_col='date'):
        """
        Appends data to storage
        :param data: Data frame to save
        :param sync_col: Index column to mask by
        """
        with self.lock:
            f = open(self.storage, 'r+')
            val = f.readline()
            f.close()
            frame = data
            if len(val) <= 1:
                frame.to_csv(self.storage)
                return
            else:
                curr = pd.read_csv(self.storage, index_col=sync_col)
            if len(curr) == 0 or curr.empty:
                frame.to_csv(self.storage)
                return
            mask = frame.index > curr.index[len(curr) - 1]
            frame = frame[mask]
            curr = pd.concat([curr, frame])
            with self.lock:
                curr.to_csv(self.storage)

    def save(self, data: pd.DataFrame):
        """
        Overrides data in storage
        """
        with self.lock:
            data.to_csv(self.storage)

    def get(self):
        """
        :return Data from the storage
        """
        with self.lock:
            try:
                records = pd.read_csv(self.storage)
            except EmptyDataError:
                records = pd.DataFrame()
            return records

    def __init_storage(self):
        """
        Creates file if it does not exist
        """
        dirs = os.path.dirname(self.storage)
        try:
            os.makedirs(dirs)
        except OSError:
            print("Creation of the directory %s failed" % dirs)
        else:
            print("Successfully created the directory %s " % dirs)

        if not os.path.isfile(self.storage):
            f = open(self.storage, "w+")
            f.close()

    def dispose(self):
        """
        Cleans the mess
        """
        try:
            os.remove(self.storage)
        except Exception:
            print('File already disposed')
            raise Exception
