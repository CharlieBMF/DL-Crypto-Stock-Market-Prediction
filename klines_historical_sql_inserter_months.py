import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from base import Api, Klines
from orderbook_functions import fill_bids_value_to_range, fill_asks_value_to_range
import time
import calendar
from datetime import datetime
from stockstats import wrap


historical_kline = Klines(base_url='https://api.binance.com')

df_sql_inserter = pd.DataFrame(columns=[
    'ISOInsertTimestamp', 'ISOTimestampKlineOPEN', 'UNIXInsertTimestamp', 'UNIXTimestampKlineOPEN',
    'UNIXTimestampKlineCLOSE', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'quoteAssetVolume',
    'tradesAmount', 'takerButBase', 'takerBuyQuote'
])

df_stock_stats = pd.DataFrame(columns=['amount', 'close', 'high', 'low', 'volume', 'open'])
periods = ['1w', '3d', '1d', '12h', '8h', '6h', '4h', '2h', '1h', '30m', '15m', '5m', '3m', '1m']
actual_period = periods[0]


for i in range(0, 1):
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
    #historical_kline.update_last_request_timestamp(actual_period)
    for record in r.json():
        print(record)
        df_sql_inserter.at[len(df_sql_inserter), 'UNIXTimestampKlineOPEN'] = record[0]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'UNIXTimestampKlineCLOSE'] = record[6]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'openPrice'] = record[1]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'highPrice'] = record[2]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'lowPrice'] = record[3]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'closePrice'] = record[4]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'volume'] = record[5]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'quoteAssetVolume'] = record[7]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'tradesAmount'] = record[8]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'takerButBase'] = record[9]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'takerBuyQuote'] = record[10]


        df_stock_stats.at[len(df_stock_stats), 'amount'] = float(record[8])
        df_stock_stats.at[len(df_stock_stats) - 1, 'close'] = float(record[4])
        df_stock_stats.at[len(df_stock_stats) - 1, 'high'] = float(record[2])
        df_stock_stats.at[len(df_stock_stats) - 1, 'low'] = float(record[3])
        df_stock_stats.at[len(df_stock_stats) - 1, 'volume'] = float(record[5])
        df_stock_stats.at[len(df_stock_stats) - 1, 'open'] = float(record[1])

        df_stock_stats['amount'] = df_stock_stats['amount'].astype(float)
        df_stock_stats['close'] = df_stock_stats['close'].astype(float)
        df_stock_stats['high'] = df_stock_stats['high'].astype(float)
        df_stock_stats['low'] = df_stock_stats['low'].astype(float)
        df_stock_stats['volume'] = df_stock_stats['volume'].astype(float)
        df_stock_stats['open'] = df_stock_stats['volume'].astype(float)

        wrapped_df = wrap(df_stock_stats)
        print(wrapped_df)
        print(wrapped_df.dtypes)

        if len(wrapped_df) > 4:
            print(wrapped_df.init_all())
