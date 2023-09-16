from numpy import ones,vstack
from numpy.linalg import lstsq
import time


""" Time unit for this class is *hours* since epoch """


class TrendLine:
    def __init__(self, t1, t2, p1, p2):
        self.t1 = t1
        self.t2 = t2
        self.p1 = p1
        self.p2 = p2
        self.a, self.b = self.get_equation_coefs()

    def get_extension_in_time(self, price, t):
        p = self.get_price_for_time(t)
        if price > p:
            return price / p - 1
        else:
            return 1 - p / price

    def get_extension_now(self, price):
        self.get_extension_in_time(price, time.time() / 60 / 60)

    def get_price_for_time(self, t):
        return self.a * t + self.b

    def get_price(self):
        return self.get_price_for_time(time.time() / 60 / 60)

    def get_time_for_price(self, p):
        return (p - self.b)/self.a

    def get_equation_coefs(self):
        pts = [(self.t1, self.p1), (self.t2, self.p2)]
        x_cords, y_cords = zip(*pts)
        X = vstack([x_cords, ones(len(y_cords))]).T
        a, b = lstsq(X, y_cords, rcond=None)[0]
        return a, b


if __name__ == '__main__':
    t = TrendLine(0, 1000, 1000, 1000)
    print(t.get_price_for_time(200))
    print(t.get_extension_in_time(180, 200))
