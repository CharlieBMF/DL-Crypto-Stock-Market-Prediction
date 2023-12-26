import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from base import Api, Klines
from orderbook_functions import fill_bids_value_to_range, fill_asks_value_to_range
import time
import calendar
from datetime import datetime


historical_kline = Klines(base_url='https://api.binance.com')

sql_columns = [
    'ISOInsertTimestamp', 'ISOTimestampKlineOPEN', 'UNIXInsertTimestamp', 'UNIXTimestampKlineOPEN',
    'UNIXTimestampKlineCLOSE', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'quoteAssetVolume',
    'tradesAmount', 'takerButBase', 'takerBuyQuote'
]

df_for_stock_stats = pd.DataFrame(columns=['amount', 'close', 'high', 'low', 'volume'])
periods = ['1w', '3d', '1d', '12h', '8h', '6h', '4h', '2h', '1h', '30m', '15m', '5m', '3m', '1m']
actual_period = periods[0]


for i in range(0, 2):
    # if historical_kline.update_period(actual_period):  # period actualization
    #     actual_period = periods[periods.index(actual_period) + 1]
    #     continue

    historical_kline.server_time = historical_kline.check_time()
    print('server', historical_kline.server_time.json())
    print('system', int(time.time() * 1000))
    print('minimal time is ', 	1501538400*1000)
#    r = historical_kline.get_klines_data(symbol='BTCUSDT', start_time=str(historical_kline.last_request_timestamp['1M'])
#                                         , end_time=str(historical_kline.last_request_timestamp['1M'] +
#                                                       historical_kline.time_intervals_in_unix['1M']), interval='1M')
    r = historical_kline.get_klines_data(symbol='BTCUSDT', start_time=str(historical_kline.last_request_timestamp['1M'])
                                        , end_time=str(int(time.time() * 1000)), interval='1M')
    print(r.json)
    print(len(r.json()))
    #historical_kline.update_last_request_timestamp(actual_period)
    print(i)
