from MaFin.Technicals.TrendLine import TrendLine


class PriceLevel(TrendLine):
    def __init__(self, p1):
        super().__init__(0, 1, p1, p1)
