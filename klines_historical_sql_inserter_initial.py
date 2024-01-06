import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from base import Api, Klines
from orderbook_functions import fill_bids_value_to_range, fill_asks_value_to_range
import time
import calendar
from datetime import datetime
from stockstats import wrap, unwrap
from sqlalchemy import create_engine, text

start = time.time()
historical_kline = Klines(base_url='https://api.binance.com', db_connection_string='postgresql://postgres:postgres@127.0.0.1/CryptoData',)

df_sql_inserter = pd.DataFrame(columns=[
    'ISOInsertTimestamp', 'ISOTimestampKlineCLOSE', 'UNIXTimestampKlineOPEN', 'UNIXTimestampKlineCLOSE',
    'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'quoteAssetVolume',
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
    r = historical_kline.get_klines_data(
        symbol='BTCUSDT',
        start_time=str(historical_kline.last_request_timestamp['1w']),
        end_time=str(historical_kline.last_request_timestamp['1w']+historical_kline.time_intervals_in_unix['1w']*500),
        #end_time=str(int(time.time())*1000),
        interval='1w'
    )
    #historical_kline.update_last_request_timestamp(actual_period)
    for record in r.json():
        print(record)
        df_sql_inserter.at[len(df_sql_inserter), 'UNIXTimestampKlineOPEN'] = record[0]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'ISOInsertTimestamp'] = datetime.utcnow()
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'ISOTimestampKlineCLOSE'] = str(
            datetime.utcfromtimestamp(int(record[6])/1000))
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'UNIXTimestampKlineCLOSE'] = record[6]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'openPrice'] = round(float(record[1]), 2)
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'highPrice'] = round(float(record[2]), 2)
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'lowPrice'] = round(float(record[3]), 2)
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'closePrice'] = round(float(record[4]), 2)
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'volume'] = round(float(record[5]), 2)
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'quoteAssetVolume'] = round(float(record[7]), 2)
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'tradesAmount'] = record[8]
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'takerButBase'] = round(float(record[9]), 2)
        df_sql_inserter.at[len(df_sql_inserter) - 1, 'takerBuyQuote'] = round(float(record[10]), 2)
        print('DF SQL INSERTER', df_sql_inserter)

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
        df_stock_stats['open'] = df_stock_stats['open'].astype(float)

        wrapped_df = wrap(df_stock_stats)
        print(wrapped_df)
        print(wrapped_df.dtypes)

    stock_stats = wrapped_df[['close_-5_d', 'close_-10_d', 'close_-15_d', 'close_-25_d', 'close_-50_d',
                                      'close_-99_d', 'rsi_6', 'rsi_14', 'rsi_25', 'rsi_35', 'rsi_55',
                                      'rsi_80', 'rsi_99',  'rsi_150', 'stochrsi_6', 'stochrsi_14', 'stochrsi_25',
                                      'stochrsi_35', 'stochrsi_55', 'stochrsi_80', 'stochrsi_99', 'stochrsi_150', 'wt1',
                                      'wt2', 'close_6_roc', 'close_14_roc', 'close_25_roc', 'close_35_roc',
                                      'close_55_roc', 'close_80_roc', 'close_99_roc', 'close_150_roc', 'close_6_mad',
                                      'close_14_mad', 'close_25_mad', 'close_35_mad', 'close_55_mad', 'close_80_mad',
                                      'close_99_mad', 'close_150_mad', 'trix', 'close_6_tema', 'close_14_tema',
                                      'close_25_tema', 'close_35_tema', 'close_55_tema', 'close_80_tema',
                                      'close_99_tema', 'close_150_tema', 'vr_6', 'vr_14', 'vr_25', 'vr_35', 'vr_55',
                                      'vr_80', 'vr_99', 'vr_150', 'wr_6', 'wr_14', 'wr_25', 'wr_35', 'wr_55', 'wr_80',
                                      'wr_99', 'wr_150', 'cci_6', 'cci_14', 'cci_25', 'cci_35', 'cci_55', 'cci_80',
                                      'cci_99', 'cci_150', 'atr_6', 'atr_14', 'atr_25', 'atr_35', 'atr_55', 'atr_80',
                                      'atr_99', 'atr_150', 'supertrend', 'supertrend_ub', 'supertrend_lb', 'dma', 'pdi',
                                      'ndi', 'dx', 'adx', 'adxr', 'kdjk', 'kdjd', 'kdjj', 'boll_6', 'boll_14',
                                      'boll_25', 'boll_35', 'boll_55', 'boll_80', 'boll_99', 'boll_150', 'macd',
                                      'macds', 'macdh', 'ppo', 'ppos', 'ppoh', 'close_6_sma', 'close_14_sma',
                                      'close_25_sma', 'close_35_sma', 'close_55_sma', 'close_80_sma', 'close_99_sma',
                                      'close_150_sma', 'close_6_mstd', 'close_14_mstd', 'close_25_mstd',
                                      'close_35_mstd', 'close_55_mstd', 'close_80_mstd', 'close_99_mstd',
                                      'close_150_mstd', 'close_6_mvar', 'close_14_mvar', 'close_25_mvar',
                                      'close_35_mvar', 'close_55_mvar', 'close_80_mvar', 'close_99_mvar',
                                      'close_150_mvar', 'vwma_6', 'vwma_14', 'vwma_25', 'vwma_35', 'vwma_55', 'vwma_80',
                                      'vwma_99', 'vwma_150', 'chop_6', 'chop_14', 'chop_25', 'chop_35', 'chop_55',
                                      'chop_80', 'chop_99', 'chop_150', 'mfi_6', 'mfi_14', 'mfi_25', 'mfi_35', 'mfi_55',
                                      'mfi_80', 'mfi_99', 'mfi_150', 'eribull_6', 'eribull_14', 'eribull_25',
                                      'eribull_35', 'eribull_55', 'eribull_80', 'eribull_99', 'eribull_150',
                                      'eribear_6', 'eribear_14', 'eribear_25', 'eribear_35', 'eribear_55', 'eribear_80',
                                      'eribear_99', 'eribear_150', 'ker_6', 'ker_14', 'ker_25', 'ker_35', 'ker_55',
                                      'ker_80', 'ker_99', 'ker_150', 'close_6_kama', 'close_14_kama', 'close_25_kama',
                                      'close_35_kama', 'close_55_kama', 'close_80_kama', 'close_99_kama',
                                      'close_150_kama', 'aroon_6', 'aroon_14', 'aroon_25', 'aroon_35', 'aroon_55',
                                      'aroon_80', 'aroon_99', 'aroon_150', 'close_6_z', 'close_14_z', 'close_25_z',
                                      'close_35_z', 'close_55_z', 'close_80_z', 'close_99_z', 'close_150_z', 'ao',
                                      'bop', 'cmo_6', 'cmo_14', 'cmo_25', 'cmo_35', 'cmo_55', 'cmo_80', 'cmo_99',
                                      'cmo_150', 'coppock', 'ichimoku',
                                      'ftr_6', 'ftr_14', 'ftr_25', 'ftr_35', 'ftr_55', 'ftr_80', 'ftr_99', 'ftr_150',
                                      'rvgi_6', 'rvgi_14', 'rvgi_25', 'rvgi_35', 'rvgi_55', 'rvgi_80', 'rvgi_99',
                                      'rvgi_150', 'kst', 'pgo_6', 'pgo_14', 'pgo_25',
                                      'pgo_35', 'pgo_55', 'pgo_80', 'pgo_99', 'pgo_150', 'psl_6', 'psl_14', 'psl_25',
                                      'psl_35', 'psl_55', 'psl_80', 'psl_99', 'psl_150', 'pvo', 'pvos', 'pvoh', 'qqe',
                                      'qqel', 'qqes'
                                      ]]
    unwrapped = unwrap(wrapped_df)
    unwrapped.rename(columns={'close_-5_d': 'close_minus5_d', 'close_-10_d': 'close_minus10_d',
                     'close_-15_d': 'close_minus15_d',  'close_-25_d': 'close_minus25_d',
                     'close_-50_d': 'close_minus50_d', 'close_-99_d': 'close_minus99_d'}, inplace=True)
    print(unwrapped.columns)
    unwrapped[['close_minus5_d', 'close_minus10_d', 'close_minus15_d', 'close_minus25_d', 'close_minus50_d',
                                      'close_minus99_d', 'rsi_6', 'rsi_14', 'rsi_25', 'rsi_35', 'rsi_55',
                                      'rsi_80', 'rsi_99',  'rsi_150', 'stochrsi_6', 'stochrsi_14', 'stochrsi_25',
                                      'stochrsi_35', 'stochrsi_55', 'stochrsi_80', 'stochrsi_99', 'stochrsi_150', 'wt1',
                                      'wt2', 'close_6_roc', 'close_14_roc', 'close_25_roc', 'close_35_roc',
                                      'close_55_roc', 'close_80_roc', 'close_99_roc', 'close_150_roc', 'close_6_mad',
                                      'close_14_mad', 'close_25_mad', 'close_35_mad', 'close_55_mad', 'close_80_mad',
                                      'close_99_mad', 'close_150_mad', 'trix', 'close_6_tema', 'close_14_tema',
                                      'close_25_tema', 'close_35_tema', 'close_55_tema', 'close_80_tema',
                                      'close_99_tema', 'close_150_tema', 'vr_6', 'vr_14', 'vr_25', 'vr_35', 'vr_55',
                                      'vr_80', 'vr_99', 'vr_150', 'wr_6', 'wr_14', 'wr_25', 'wr_35', 'wr_55', 'wr_80',
                                      'wr_99', 'wr_150', 'cci_6', 'cci_14', 'cci_25', 'cci_35', 'cci_55', 'cci_80',
                                      'cci_99', 'cci_150', 'atr_6', 'atr_14', 'atr_25', 'atr_35', 'atr_55', 'atr_80',
                                      'atr_99', 'atr_150', 'supertrend', 'supertrend_ub', 'supertrend_lb', 'dma', 'pdi',
                                      'ndi', 'dx', 'adx', 'adxr', 'kdjk', 'kdjd', 'kdjj', 'boll_6', 'boll_14',
                                      'boll_25', 'boll_35', 'boll_55', 'boll_80', 'boll_99', 'boll_150', 'macd',
                                      'macds', 'macdh', 'ppo', 'ppos', 'ppoh', 'close_6_sma', 'close_14_sma',
                                      'close_25_sma', 'close_35_sma', 'close_55_sma', 'close_80_sma', 'close_99_sma',
                                      'close_150_sma', 'close_6_mstd', 'close_14_mstd', 'close_25_mstd',
                                      'close_35_mstd', 'close_55_mstd', 'close_80_mstd', 'close_99_mstd',
                                      'close_150_mstd', 'close_6_mvar', 'close_14_mvar', 'close_25_mvar',
                                      'close_35_mvar', 'close_55_mvar', 'close_80_mvar', 'close_99_mvar',
                                      'close_150_mvar', 'vwma_6', 'vwma_14', 'vwma_25', 'vwma_35', 'vwma_55', 'vwma_80',
                                      'vwma_99', 'vwma_150', 'chop_6', 'chop_14', 'chop_25', 'chop_35', 'chop_55',
                                      'chop_80', 'chop_99', 'chop_150', 'mfi_6', 'mfi_14', 'mfi_25', 'mfi_35', 'mfi_55',
                                      'mfi_80', 'mfi_99', 'mfi_150', 'eribull_6', 'eribull_14', 'eribull_25',
                                      'eribull_35', 'eribull_55', 'eribull_80', 'eribull_99', 'eribull_150',
                                      'eribear_6', 'eribear_14', 'eribear_25', 'eribear_35', 'eribear_55', 'eribear_80',
                                      'eribear_99', 'eribear_150', 'ker_6', 'ker_14', 'ker_25', 'ker_35', 'ker_55',
                                      'ker_80', 'ker_99', 'ker_150', 'close_6_kama', 'close_14_kama', 'close_25_kama',
                                      'close_35_kama', 'close_55_kama', 'close_80_kama', 'close_99_kama',
                                      'close_150_kama', 'aroon_6', 'aroon_14', 'aroon_25', 'aroon_35', 'aroon_55',
                                      'aroon_80', 'aroon_99', 'aroon_150', 'close_6_z', 'close_14_z', 'close_25_z',
                                      'close_35_z', 'close_55_z', 'close_80_z', 'close_99_z', 'close_150_z', 'ao',
                                      'bop', 'cmo_6', 'cmo_14', 'cmo_25', 'cmo_35', 'cmo_55', 'cmo_80', 'cmo_99',
                                      'cmo_150', 'coppock', 'ichimoku',
                                      'ftr_6', 'ftr_14', 'ftr_25', 'ftr_35', 'ftr_55', 'ftr_80', 'ftr_99', 'ftr_150',
                                      'rvgi_6', 'rvgi_14', 'rvgi_25', 'rvgi_35', 'rvgi_55', 'rvgi_80', 'rvgi_99',
                                      'rvgi_150', 'kst', 'pgo_6', 'pgo_14', 'pgo_25',
                                      'pgo_35', 'pgo_55', 'pgo_80', 'pgo_99', 'pgo_150', 'psl_6', 'psl_14', 'psl_25',
                                      'psl_35', 'psl_55', 'psl_80', 'psl_99', 'psl_150', 'pvo', 'pvos', 'pvoh', 'qqe',
                                      'qqel', 'qqes'
                                      ]] = unwrapped[['close_minus5_d', 'close_minus10_d', 'close_minus15_d', 'close_minus25_d', 'close_minus50_d',
                                      'close_minus99_d', 'rsi_6', 'rsi_14', 'rsi_25', 'rsi_35', 'rsi_55',
                                      'rsi_80', 'rsi_99',  'rsi_150', 'stochrsi_6', 'stochrsi_14', 'stochrsi_25',
                                      'stochrsi_35', 'stochrsi_55', 'stochrsi_80', 'stochrsi_99', 'stochrsi_150', 'wt1',
                                      'wt2', 'close_6_roc', 'close_14_roc', 'close_25_roc', 'close_35_roc',
                                      'close_55_roc', 'close_80_roc', 'close_99_roc', 'close_150_roc', 'close_6_mad',
                                      'close_14_mad', 'close_25_mad', 'close_35_mad', 'close_55_mad', 'close_80_mad',
                                      'close_99_mad', 'close_150_mad', 'trix', 'close_6_tema', 'close_14_tema',
                                      'close_25_tema', 'close_35_tema', 'close_55_tema', 'close_80_tema',
                                      'close_99_tema', 'close_150_tema', 'vr_6', 'vr_14', 'vr_25', 'vr_35', 'vr_55',
                                      'vr_80', 'vr_99', 'vr_150', 'wr_6', 'wr_14', 'wr_25', 'wr_35', 'wr_55', 'wr_80',
                                      'wr_99', 'wr_150', 'cci_6', 'cci_14', 'cci_25', 'cci_35', 'cci_55', 'cci_80',
                                      'cci_99', 'cci_150', 'atr_6', 'atr_14', 'atr_25', 'atr_35', 'atr_55', 'atr_80',
                                      'atr_99', 'atr_150', 'supertrend', 'supertrend_ub', 'supertrend_lb', 'dma', 'pdi',
                                      'ndi', 'dx', 'adx', 'adxr', 'kdjk', 'kdjd', 'kdjj', 'boll_6', 'boll_14',
                                      'boll_25', 'boll_35', 'boll_55', 'boll_80', 'boll_99', 'boll_150', 'macd',
                                      'macds', 'macdh', 'ppo', 'ppos', 'ppoh', 'close_6_sma', 'close_14_sma',
                                      'close_25_sma', 'close_35_sma', 'close_55_sma', 'close_80_sma', 'close_99_sma',
                                      'close_150_sma', 'close_6_mstd', 'close_14_mstd', 'close_25_mstd',
                                      'close_35_mstd', 'close_55_mstd', 'close_80_mstd', 'close_99_mstd',
                                      'close_150_mstd', 'close_6_mvar', 'close_14_mvar', 'close_25_mvar',
                                      'close_35_mvar', 'close_55_mvar', 'close_80_mvar', 'close_99_mvar',
                                      'close_150_mvar', 'vwma_6', 'vwma_14', 'vwma_25', 'vwma_35', 'vwma_55', 'vwma_80',
                                      'vwma_99', 'vwma_150', 'chop_6', 'chop_14', 'chop_25', 'chop_35', 'chop_55',
                                      'chop_80', 'chop_99', 'chop_150', 'mfi_6', 'mfi_14', 'mfi_25', 'mfi_35', 'mfi_55',
                                      'mfi_80', 'mfi_99', 'mfi_150', 'eribull_6', 'eribull_14', 'eribull_25',
                                      'eribull_35', 'eribull_55', 'eribull_80', 'eribull_99', 'eribull_150',
                                      'eribear_6', 'eribear_14', 'eribear_25', 'eribear_35', 'eribear_55', 'eribear_80',
                                      'eribear_99', 'eribear_150', 'ker_6', 'ker_14', 'ker_25', 'ker_35', 'ker_55',
                                      'ker_80', 'ker_99', 'ker_150', 'close_6_kama', 'close_14_kama', 'close_25_kama',
                                      'close_35_kama', 'close_55_kama', 'close_80_kama', 'close_99_kama',
                                      'close_150_kama', 'aroon_6', 'aroon_14', 'aroon_25', 'aroon_35', 'aroon_55',
                                      'aroon_80', 'aroon_99', 'aroon_150', 'close_6_z', 'close_14_z', 'close_25_z',
                                      'close_35_z', 'close_55_z', 'close_80_z', 'close_99_z', 'close_150_z', 'ao',
                                      'bop', 'cmo_6', 'cmo_14', 'cmo_25', 'cmo_35', 'cmo_55', 'cmo_80', 'cmo_99',
                                      'cmo_150', 'coppock', 'ichimoku',
                                      'ftr_6', 'ftr_14', 'ftr_25', 'ftr_35', 'ftr_55', 'ftr_80', 'ftr_99', 'ftr_150',
                                      'rvgi_6', 'rvgi_14', 'rvgi_25', 'rvgi_35', 'rvgi_55', 'rvgi_80', 'rvgi_99',
                                      'rvgi_150', 'kst', 'pgo_6', 'pgo_14', 'pgo_25',
                                      'pgo_35', 'pgo_55', 'pgo_80', 'pgo_99', 'pgo_150', 'psl_6', 'psl_14', 'psl_25',
                                      'psl_35', 'psl_55', 'psl_80', 'psl_99', 'psl_150', 'pvo', 'pvos', 'pvoh', 'qqe',
                                      'qqel', 'qqes'
                                      ]].round(5)






calculated_stock_stats = unwrapped.drop(columns=['amount', 'close', 'high', 'low', 'volume', 'open'])

df_sql_inserter_concat = pd.concat([df_sql_inserter, calculated_stock_stats], axis=1, ignore_index=False)
with pd.ExcelWriter(r'stock_stats.xlsx') as writer:
    df_sql_inserter_concat.to_excel(writer)

engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')


#df_sql_inserter_concat.to_sql('BTC_KLINES_CACHE_1MONTH', con=engine, if_exists='replace', index=False)

# with engine.connect() as conn:
#     for index, row in df_sql_inserter_concat.iterrows():
#         values = ", ".join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
#         query = f'INSERT INTO "CACHE_BTC_1M" ("{", ".join(df_sql_inserter_concat.columns)}") VALUES ({values})'
#       #  query = 'INSERT INTO "CACHE_BTC_1M" (', ", ".join(df_sql_inserter_concat.columns), ') VALUES (', values,')'
#         print(query)
#         result = conn.execute(text(query))


engine.dispose()
print("Data inserted successfully!")

print('time', time.time()-start)