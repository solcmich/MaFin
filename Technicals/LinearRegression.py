import numpy as np
from sklearn.linear_model import LinearRegression

from MaFin.Technicals.TrendLine import TrendLine


def get_linear_regression(pts):
    line = LinearRegression().fit(pts[0], pts[1])
    coefs = line.coef_
    return TrendLine(0, coefs[0], 0, coefs[1])
