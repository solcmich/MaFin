
class Strategy:
    def __init__(self, initial_data, asset, a1_a2_max_dollar_ratio, a1_a2_min_dollar_ratio):
        self.a1_a2_max_dollar_ratio = a1_a2_max_dollar_ratio
        self.a1_a2_min_dollar_ratio = a1_a2_min_dollar_ratio
        self.initial_data = initial_data
        self.asset = asset
        self.subs = []

    def subscribe_for_signal(self, sub):
        self.subs.append(sub)

    def ping_data(self, data):
        self.initial_data.append(data)
        # todo: Do smth with the data, check for signal and stuff
        pass






