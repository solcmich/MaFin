from MaFin.Data.Storage.ColdStorage import ColdStorage
import threading


class SelfUpdatedStorage(ColdStorage, threading.Thread):
    def __init__(self, storage_path, data_endpoint, hot_event, wait_functor=lambda: 3600, **endpoint_params):
        ColdStorage.__init__(self, storage_path)
        threading.Thread.__init__(self)
        self.data_endpoint = data_endpoint
        self.endpoint_params = endpoint_params
        self.hot_event = hot_event
        self.wait_functor = wait_functor

    def run(self):
        """
        Cleverly handles data retrieval with help of hot events
        """
        self.save(self.data_endpoint(**self.endpoint_params))
        while True:
            self.hot_event.wait(self.wait_functor())
            self.append(self.data_endpoint(**self.endpoint_params))
            self.hot_event.clear()
