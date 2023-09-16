import datetime

from MaFin.Client.BinanceClient import BinanceClient


def get_seconds_to_kline_close(tf):
    _, _, _, hour, minute = map(int, datetime.datetime.utcnow().strftime("%Y %m %d %H %M").split())
    weekday = datetime.datetime.utcnow().today().weekday()
    if tf == '1h':
        return (60 - minute) * 60
    elif tf == '4h':
        return (3 - (hour % 4)) * 3600 + (60 - minute) * 60
    elif tf == '1d':
        return (23 - hour) * 3600 + (60 - minute) * 60
    elif tf == '1w':
        return (6 - weekday) * 3600 * 24 + (23 - hour) * 3600 + (60 - minute) * 60

    return 60
