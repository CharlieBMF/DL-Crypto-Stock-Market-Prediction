import time
from sqlalchemy import create_engine, text
import requests
import urllib.parse
import pandas as pd
from datetime import datetime
import json
from stockstats import wrap, unwrap


class Api:

    def __init__(self, base_url):
        self.base_url = base_url
        self.check_time_url = '/api/v3/time'
        self.avg_price_url = '/api/v3/avgPrice'
        self.order_book_url = 'api/v3/depth'
        self.recent_trades_url = 'api/v3/trades'
        self.historical_trades_url = 'api/v3/historicalTrades'
        self.klines_url = '/api/v3/klines'
        self.last24hr_url = '/api/v3/ticker/24hr'
        self.actual_price_ticker_url = '/api/v3/ticker/price'
        self.order_book_ticker_url = '/api/v3/ticker/bookTicker'
        self.price_change_ticker_url = '/api/v3/ticker'

    def check_time(self):
        url = urllib.parse.urljoin(self.base_url, self.check_time_url)
        print(url)
        response = self.execute_request(url)
        return response

    def get_avg_price(self, symbol):
        """
        Price avg for last 5 mins
        :param symbol:
        :return:
        """
        symbol_url = '?symbol=' + str(symbol).replace("'", "\"")
        url = urllib.parse.urljoin(self.base_url, self.avg_price_url) + symbol_url
        print(url)
        response = self.execute_request(url)
        print(response.text)
        return response

    def get_order_book(self, symbol, limit='100'):
        """
        :param symbol: ex. ["BTCUSDT"]
        :param limit: 	Default 100; max 5000. If limit > 5000, then the response will truncate to 5000.
        :return:
                {
                  "lastUpdateId": 1027024,
                  "bids": [
                    [
                      "4.00000000",     // PRICE
                      "431.00000000"    // QTY
                    ]
                  ],
                  "asks": [
                    [
                      "4.00000200",
                      "12.00000000"
                    ]
                  ]
                }
        """
        symbol_url = '?symbol=' + str(symbol).replace("'", "\"")
        limit_url = '&limit=' + limit
        url = urllib.parse.urljoin(self.base_url, self.order_book_url) + symbol_url + limit_url
        print(url)
        response = self.execute_request(url)
        #print(response.text)
        return response

    def get_recent_trade(self, symbol, limit='500'):
        """
        Get recent trades.
        :param symbol: ex. ["BTCUSDT"]
        :param limit: Default 500; max 1000
        :return:   {
                    "id": 28457,
                    "price": "4.00000100",
                    "qty": "12.00000000",
                    "quoteQty": "48.000012",
                    "time": 1499865549590,
                    "isBuyerMaker": true,
                    "isBestMatch": true
                  }
        """
        symbol_url = '?symbol=' + str(symbol).replace("'", "\"")
        limit_url = '&limit=' + limit
        url = urllib.parse.urljoin(self.base_url, self.recent_trades_url) + symbol_url + limit_url
        print(url)
        response = self.execute_request(url)
        print(response.text)
        return response

    def get_historical_trades(self, symbol, limit='500'):
        """
        Get older market trades.
        :param symbol: ex. ["BTCUSDT"]
        :param limit: Default 500; max 1000
        :return:  {
                    "id": 28457,
                    "price": "4.00000100",
                    "qty": "12.00000000",
                    "quoteQty": "48.000012",
                    "time": 1499865549590, // Trade executed timestamp, as same as T in the stream
                    "isBuyerMaker": true,
                    "isBestMatch": true
                  }
        """
        symbol_url = '?symbol=' + str(symbol).replace("'", "\"")
        limit_url = '&limit=' + limit
        url = urllib.parse.urljoin(self.base_url, self.historical_trades_url) + symbol_url + limit_url
        print(url)
        response = self.execute_request(url)
        print(response.text)
        return response

    def get_klines_data(self, symbol, start_time, end_time, interval, limit='500'):
        """
        Kline/candlestick bars for a symbol.
        :param symbol: ex. ["BTCUSDT"]
        :param interval:s-> seconds; m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
                        1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
        :param limit: Default 500; max 1000
        :return:  [
                    1499040000000,      // Kline open time
                    "0.01634790",       // Open price
                    "0.80000000",       // High price
                    "0.01575800",       // Low price
                    "0.01577100",       // Close price
                    "148976.11427815",  // Volume
                    1499644799999,      // Kline Close time
                    "2434.19055334",    // Quote asset volume
                    308,                // Number of trades
                    "1756.87402397",    // Taker buy base asset volume
                    "28.46694368",      // Taker buy quote asset volume
                    "0"                 // Unused field, ignore.
                  ]
        """
        symbol_url = '?symbol=' + str(symbol).replace("'", "\"")
        interval_url = '&interval=' + interval
        start_time_url = '&startTime=' + start_time
        end_time_url = '&endTime=' + end_time
        limit_url = '&limit=' + limit
        url = (urllib.parse.urljoin(self.base_url, self.klines_url) + symbol_url + interval_url + start_time_url +
               end_time_url + limit_url)
        print(url)
        response = self.execute_request(url)
        #print(response.text)
        return response

    def get_last_24hr(self, symbols):
        """
        24h rolling window price change statistics. Careful when accessing this with no symbol.
        :param symbols: ex. ["BTCUSDT","BNBUSDT"]
        :return:
                {
                  "symbol": "BNBBTC",
                  "priceChange": "-94.99999800",
                  "priceChangePercent": "-95.960",
                  "weightedAvgPrice": "0.29628482",
                  "prevClosePrice": "0.10002000",
                  "lastPrice": "4.00000200",
                  "lastQty": "200.00000000",
                  "bidPrice": "4.00000000",
                  "bidQty": "100.00000000",
                  "askPrice": "4.00000200",
                  "askQty": "100.00000000",
                  "openPrice": "99.00000000",
                  "highPrice": "100.00000000",
                  "lowPrice": "0.10000000",
                  "volume": "8913.30000000",
                  "quoteVolume": "15.30000000",
                  "openTime": 1499783499040,
                  "closeTime": 1499869899040,
                  "firstId": 28385,   // First tradeId
                  "lastId": 28460,    // Last tradeId
                  "count": 76         // Trade count
                }
        """
        symbols_url = '?symbols=' + str(symbols).replace("'", "\"")
        url = urllib.parse.urljoin(self.base_url, self.last24hr_url) + symbols_url
        print(url)
        response = self.execute_request(url)
        print(response.text)
        return response

    def get_actual_price_ticker(self, symbols):
        """
        Latest price for a symbol or symbols.
        :param symbols:ex. ["BTCUSDT","BNBUSDT"]
        :return:
                {
                  "symbol": "LTCBTC",
                  "price": "4.00000200"
                }
        """
        symbols_url = '?symbols=' + str(symbols).replace("'", "\"")
        url = urllib.parse.urljoin(self.base_url, self.actual_price_ticker_url) + symbols_url
        print(url)
        response = self.execute_request(url)
        print(response.text)
        return response

    def get_book_ticker(self, symbols):
        """
        Best price/qty on the order book for a symbol or symbols.
        Latest price for a symbol or symbols.
        :param symbols: ex. ["BTCUSDT","BNBUSDT"]
        :return:
        {
          "symbol": "LTCBTC",
          "bidPrice": "4.00000000",
          "bidQty": "431.00000000",
          "askPrice": "4.00000200",
          "askQty": "9.00000000"
        }
        """
        symbols_url = '?symbols=' + str(symbols).replace("'", "\"")
        url = urllib.parse.urljoin(self.base_url, self.order_book_ticker_url) + symbols_url
        print(url)
        response = self.execute_request(url)
        print(response.text)

    def get_price_change_statistic(self, symbols , windowsize='1d'):
        """
        The window used to compute statistics will be no more than 59999ms from the requested windowSize.
        openTime for /api/v3/ticker always starts on a minute, while the closeTime is the current time of the request.
         As such, the effective window will be up to 59999ms wider than windowSize.
        E.g. If the closeTime is 1641287867099 (January 04, 2022 09:17:47:099 UTC) , and the windowSize is 1d.
        the openTime will be: 1641201420000 (January 3, 2022, 09:17:00 UTC)

        :param symbols: ex. ["BTCUSDT","BNBUSDT"]
        :param windowsize: Defaults to 1d if no parameter provided
                        Supported windowSize values:
                        1m,2m....59m for minutes
                        1h, 2h....23h - for hours
                        1d...7d - for days
                        Units cannot be combined (e.g. 1d2h is not allowed)
        :return:  {
                    "symbol": "BTCUSDT",
                    "priceChange": "-154.13000000",        // Absolute price change
                    "priceChangePercent": "-0.740",        // Relative price change in percent
                    "weightedAvgPrice": "20677.46305250",  // QuoteVolume / Volume
                    "openPrice": "20825.27000000",
                    "highPrice": "20972.46000000",
                    "lowPrice": "20327.92000000",
                    "lastPrice": "20671.14000000",
                    "volume": "72.65112300",
                    "quoteVolume": "1502240.91155513",     // Sum of (price * volume) for all trades
                    "openTime": 1655432400000,             // Open time for ticker window
                    "closeTime": 1655446835460,            // Close time for ticker window
                    "firstId": 11147809,                   // Trade IDs
                    "lastId": 11149775,
                    "count": 1967                          // Number of trades in the interval
  }
        """
        print(symbols)
        symbols_url = '?symbols=' + str(symbols).replace("'", "\"")
        print(symbols_url)
        windowsize_url = '&windowSize=' + windowsize
        url = urllib.parse.urljoin(self.base_url, self.price_change_ticker_url) + symbols_url + windowsize_url
        print(url)
        response = self.execute_request(url)
        print(response.text)
        return response

    @staticmethod
    def execute_request(url):
        response = requests.get(url)
        return response


class Klines(Api):

    def __init__(self, base_url, db_connection_string, coin):
        self.server_time = 1503273600000  # 21.08.2017
        self.coin = coin
        self.db_connection_string = db_connection_string
        self.next_table_name = ''
        self.table_creation_rules = {
            'never': ['1w', '3d', '1d', '12h', '8h'], 'yearly': ['6h', '4h', '2h', '1h', '30m', '15m',],
            'monthly': ['5m', '3m', '1m']
        }
        self.possible_periods = ['1w', '3d', '1d', '12h', '8h', '6h', '4h', '2h', '1h', '30m', '15m', '5m', '3m', '1m']
        self.actual_period = '1w'
        self.last_request_timestamp = {
            '1m': 1503273600000, '3m': 1503273600000, '5m': 1503273600000,'15m': 1503273600000, '30m': 1503273600000,
            '1h': 1503273600000, '2h': 1503273600000, '4h': 1503273600000, '6h': 1503273600000, '8h': 1503273600000,
            '12h': 1503273600000, '1d': 1503273600000, '3d': 1503273600000, '1w': 1503273600000, #'1M': 1503273600000
        }  # 21.08.2017
        self.next_request_timestamp = {
            '1m': 1503273600000, '3m': 1503273600000, '5m': 1503273600000,'15m': 1503273600000, '30m': 1503273600000,
            '1h': 1503273600000, '2h': 1503273600000, '4h': 1503273600000, '6h': 1503273600000, '8h': 1503273600000,
            '12h': 1503273600000, '1d': 1503273600000, '3d': 1503273600000, '1w': 1503273600000, #'1M': 1503273600000
        }  # 21.08.2017
        self.time_intervals_in_unix = {
            '1m': 60000, '3m': 180000, '5m': 300000, '15m': 900000, '30m': 1800000, '1h': 3600000, '2h': 7200000,
            '4h': 14400000, '6h': 21600000, '8h': 28800000, '12h': 43200000, '1d': 86400000, '3d': 259200000,
            '1w': 604800000,
        }
        self.actual_table_name = {
            '1m': '', '3m': '', '5m': '', '15m': '', '30m': '', '1h': '', '2h': '', '4h': '', '6h': '', '8h': '',
            '12h': '', '1d': '', '3d': '', '1w': '', #'1M': ''
        }
        self.next_table_name = {
            '1m': '', '3m': '', '5m': '', '15m': '', '30m': '', '1h': '', '2h': '', '4h': '', '6h': '', '8h': '',
            '12h': '', '1d': '', '3d': '', '1w': '', #'1M': ''
        }
        self.existed_tables = {
            '1m': '', '3m': '', '5m': '', '15m': '', '30m': '', '1h': '', '2h': '', '4h': '', '6h': '', '8h': '',
            '12h': '', '1d': '', '3d': '', '1w': '', #'1M': ''
        }
        self.sql_main_columns = [
            'ISOInsertTimestamp', 'ISOTimestampKlineCLOSE', 'UNIXTimestampKlineOPEN', 'UNIXTimestampKlineCLOSE',
            'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'quoteAssetVolume',
            'tradesAmount', 'takerButBase', 'takerBuyQuote'
        ]
        self.cached_last_150_rows = pd.DataFrame()
        super().__init__(base_url)

    def update_server_time(self):
        server_response = super().check_time()
        self.server_time = int(server_response.json()['serverTime'])
        print('self server time', self.server_time)

    def get_data_from_sql_checkpoints(self):
        query = text(f"SELECT * FROM \"_CHECKPOINTS\" WHERE \"Coin\" = '{self.coin}'")
        checkpoints = self.execute_db_query(query, 'select')
        for period in self.possible_periods:
            last_request_timestamp = (
                checkpoints.loc[checkpoints['Interval'] == period]['UNIXLastCloseTimestamp'].values)[0]
            next_request_timestamp = (
                checkpoints.loc[checkpoints['Interval'] == period]['UNIXNextTimestamp'].values)[0]
            actual_table_name = checkpoints.loc[checkpoints['Interval'] == period]['lastTableName'].values[0]
            next_table_name = checkpoints.loc[checkpoints['Interval'] == period]['nextTableName'].values[0]
            existed_tables = checkpoints.loc[checkpoints['Interval'] == period]['existedTables'].values[0]
            print(f'For period {period} last request timestamp = {last_request_timestamp},'
                  f'next request timestamp =  {next_request_timestamp}, {actual_table_name}, {next_table_name}')
            self.last_request_timestamp[period] = int(last_request_timestamp)
            self.next_request_timestamp[period] = int(next_request_timestamp)
            self.actual_table_name[period] = str(actual_table_name)
            self.next_table_name[period] = str(next_table_name)
            self.existed_tables[period] = json.loads(existed_tables)


    def get_data_from_sql_cache(self):
        query = "SELECT * FROM \"" + self.coin + "_KLINES_CACHE_" + self.actual_period + "\""
        cached_last_150_rows = self.execute_db_query(query, 'select')
        self.cached_last_150_rows = cached_last_150_rows[self.sql_main_columns].copy()

    def check_necessary_to_create_a_table(self):
        actual_year = self.get_self_requests_years_and_months()['actual_request_year_string']
        actual_month = self.get_self_requests_years_and_months()['actual_request_month_string']
        print('SELF ACTUAL TABLE NAME', self.actual_table_name[self.actual_period])
        print('SELF NEXT TABLE NAME', self.next_table_name[self.actual_period])
        print('ACTUAL MONTH', actual_month)
        if self.actual_period in self.table_creation_rules['yearly']:
            if actual_year not in self.actual_table_name[self.actual_period]:
                return True
        if self.actual_period in self.table_creation_rules['monthly']:
            if (actual_year not in self.actual_table_name[self.actual_period] or
                    actual_month not in self.actual_table_name[self.actual_period]):
                return True
        return False

    def create_table_for_period(self):
        query = ("CREATE TABLE \"" + self.next_table_name[self.actual_period] +
                 "\" AS SELECT * FROM \"" + self.actual_table_name[self.actual_period] + "\" WITH NO DATA")
        self.execute_db_query(query, type_of_query='insert')

    def update_kline_settings(self, type_of_update):
        if type_of_update == 'tables':
            self.actual_table_name[self.actual_period] = self.next_table_name[self.actual_period]
            data = self.get_self_requests_years_and_months()
            if self.actual_period in self.table_creation_rules['yearly']:
                self.next_table_name[self.actual_period] = (self.coin + '_KLINES_YEARLY_' +
                                                            data['next_request_year'] + '_PER_' + self.actual_period)
            elif self.actual_period in self.table_creation_rules['monthly']:
                self.next_table_name[self.actual_period] = (self.coin + '_KLINES_MONTHLY_' +
                                                            data['next_request_month'] + '_' +
                                                            data['next_request_year'] + '_PER_' + self.actual_period)
            self.existed_tables[self.actual_period].append(self.actual_table_name[self.actual_period])
            print('generated next table name: ', self.next_table_name[self.actual_period])
            #print('existed tables', self.existed_tables[self.actual_period])

    def continue_actual_period(self):
        if (self.last_request_timestamp[self.actual_period] + self.time_intervals_in_unix[self.actual_period]
                > self.server_time):
            print(f'{self.last_request_timestamp[self.actual_period]} + '
                  f'{self.time_intervals_in_unix[self.actual_period]} > '
                  f'{self.server_time})')
            return False
        return True

    def change_period(self):
        print('Changing actual period.. ', self.actual_period)
        index = self.possible_periods.index(self.actual_period)
        index = (index + 1) % len(self.possible_periods)
        self.actual_period = self.possible_periods[index]
        print('Actual period changed to...', self.actual_period)

    def get_self_requests_years_and_months(self):
        actual_request_year = datetime.utcfromtimestamp(self.last_request_timestamp[self.actual_period] / 1000).year
        actual_request_month = datetime.utcfromtimestamp(self.last_request_timestamp[self.actual_period] / 1000).month
        if self.actual_period in self.table_creation_rules['monthly']:
            if actual_request_month == 12:
                next_request_month = str(1)
                next_request_year = str(int(actual_request_year) + 1)
            else:
                next_request_month = str(int(actual_request_month) + 1)
                next_request_year = str(actual_request_year)
        else:
            next_request_month = str(actual_request_month)
            next_request_year = str(int(actual_request_year) + 1)
        return {
            'actual_request_year_string': '_' + str(actual_request_year) + '_',
            'actual_request_month_string': '_' + str(actual_request_month) + '_',
            'next_request_year': next_request_year,
            'next_request_month': next_request_month,
        }

    def get_and_save_new_klines(self):
        if self.confirm_maximal_json_lenght():
            lenght = 500
        else:
            lenght = self.get_intervals_quantity()
        print('length', lenght)
        r = self.get_klines_data(
            symbol= self.coin + 'USDT',
            start_time=str(self.last_request_timestamp[self.actual_period]),
            end_time=str(
                self.last_request_timestamp[self.actual_period] +
                self.time_intervals_in_unix[self.actual_period] * lenght
            ),
            interval=self.actual_period
        )
        df_recent_data_without_stockstats = self.generate_df_from_api_response(r)

        df_sql_inserter_concat_without_stockstats = (
            pd.concat([self.cached_last_150_rows, df_recent_data_without_stockstats], axis=0, ignore_index=True))

        df_sql_inserter_concat_with_stockstats = (
            self.calculate_stockstats_for_df(df_sql_inserter_concat_without_stockstats))

        df_new_data_to_insert = df_sql_inserter_concat_with_stockstats.iloc[150:].copy()
        df_new_data_to_cache = df_sql_inserter_concat_with_stockstats.tail(150).copy()

        self.save_klines_data(df_new_data_to_insert)
        #self.truncate_cache_data()
        self.save_cache_data(df_new_data_to_cache)
        self.last_request_timestamp[self.actual_period] = int(df_new_data_to_insert.iloc[-1]['UNIXTimestampKlineCLOSE'])
        self.next_request_timestamp[self.actual_period] = (
                self.last_request_timestamp[self.actual_period] + self.time_intervals_in_unix[self.actual_period])

        with pd.ExcelWriter(r'test.xlsx') as writer:
            df_new_data_to_insert.to_excel(writer)

        with pd.ExcelWriter(r'cachetest.xlsx') as writer:
            df_new_data_to_cache.to_excel(writer)
    def generate_df_from_api_response(self, response):
        df_sql_inserter = pd.DataFrame(columns=self.sql_main_columns)
        for record in response.json():
            df_sql_inserter.at[len(df_sql_inserter), 'UNIXTimestampKlineOPEN'] = record[0]
            df_sql_inserter.at[len(df_sql_inserter) - 1, 'ISOInsertTimestamp'] = datetime.utcnow()
            df_sql_inserter.at[len(df_sql_inserter) - 1, 'ISOTimestampKlineCLOSE'] = str(
                datetime.utcfromtimestamp(int(record[6]) / 1000))
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
        return df_sql_inserter

    def save_klines_data(self, df):
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')
        df.to_sql(self.actual_table_name[self.actual_period], con=engine, if_exists='append', index=False)
        engine.dispose()

    def truncate_cache_data(self):
        query = "TRUNCATE TABLE \"" + self.coin + "_KLINES_CACHE_" + self.actual_period + "\""
        self.execute_db_query(query, type_of_query='truncate')

    def save_cache_data(self, df):
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')
        actual_cache_table = self.coin + "_KLINES_CACHE_" + self.actual_period
        df.to_sql(actual_cache_table, con=engine, if_exists='replace', index=False)
        engine.dispose()

    def update_checkpoints_table_in_sql(self):
        #print(self.existed_tables[self.actual_period])
        query = ("UPDATE \"_CHECKPOINTS\" SET \"UNIXLastCloseTimestamp\" = " +
                 str(self.last_request_timestamp[self.actual_period]) +
                 ", \"UNIXNextTimestamp\" = " + str(self.next_request_timestamp[self.actual_period]) +
                 ", \"updateTimestamp\" = '" + str(datetime.utcnow()) +
                 "', \"lastTableName\" = '" + self.actual_table_name[self.actual_period] +
                 "', \"nextTableName\" = '" + self.next_table_name[self.actual_period] +
                 "', \"existedTables\" = '" +
                 str(self.existed_tables[self.actual_period]).replace('\\','').replace('\'','\"') +
                 "' WHERE \"Coin\" = '" + self.coin + "' AND \"Interval\" = '" + self.actual_period + "'"
                 )
        #print(query)
        self.execute_db_query(query, type_of_query='update')

    def confirm_maximal_json_lenght(self):
        if (self.last_request_timestamp[self.actual_period] + self.time_intervals_in_unix[self.actual_period] * 500 >
                self.server_time):
            return False
        return True

    def get_intervals_quantity(self):
        for i in range(1, 500):
            if (self.last_request_timestamp[self.actual_period] + self.time_intervals_in_unix[self.actual_period]
                    * i > self.server_time):
                length = i - 1
                break
        return length


    def calculate_stockstats_for_df(self, df):
        df_stock_stats = pd.DataFrame(columns=['amount', 'close', 'high', 'low', 'volume', 'open'])
        df_stock_stats['amount'] = df['tradesAmount']
        df_stock_stats['close'] = df['closePrice']
        df_stock_stats['high'] = df['highPrice']
        df_stock_stats['low'] = df['lowPrice']
        df_stock_stats['volume'] = df['volume']
        df_stock_stats['open'] = df['openPrice']

        df_stock_stats['amount'] = df_stock_stats['amount'].astype(float)
        df_stock_stats['close'] = df_stock_stats['close'].astype(float)
        df_stock_stats['high'] = df_stock_stats['high'].astype(float)
        df_stock_stats['low'] = df_stock_stats['low'].astype(float)
        df_stock_stats['volume'] = df_stock_stats['volume'].astype(float)
        df_stock_stats['open'] = df_stock_stats['open'].astype(float)

        wrapped_df = wrap(df_stock_stats)
        var = wrapped_df[['close_-5_d', 'close_-10_d', 'close_-15_d', 'close_-25_d', 'close_-50_d',
                          'close_-99_d', 'rsi_6', 'rsi_14', 'rsi_25', 'rsi_35', 'rsi_55',
                          'rsi_80', 'rsi_99', 'rsi_150', 'stochrsi_6', 'stochrsi_14', 'stochrsi_25',
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
                                  'close_-15_d': 'close_minus15_d', 'close_-25_d': 'close_minus25_d',
                                  'close_-50_d': 'close_minus50_d', 'close_-99_d': 'close_minus99_d'}, inplace=True)
        unwrapped[['close_minus5_d', 'close_minus10_d', 'close_minus15_d', 'close_minus25_d', 'close_minus50_d',
                   'close_minus99_d', 'rsi_6', 'rsi_14', 'rsi_25', 'rsi_35', 'rsi_55',
                   'rsi_80', 'rsi_99', 'rsi_150', 'stochrsi_6', 'stochrsi_14', 'stochrsi_25',
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
                   ]] = unwrapped[
            ['close_minus5_d', 'close_minus10_d', 'close_minus15_d', 'close_minus25_d', 'close_minus50_d',
             'close_minus99_d', 'rsi_6', 'rsi_14', 'rsi_25', 'rsi_35', 'rsi_55',
             'rsi_80', 'rsi_99', 'rsi_150', 'stochrsi_6', 'stochrsi_14', 'stochrsi_25',
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

        calculated_and_concated_with_primary_df = (
            pd.concat([df, calculated_stock_stats], axis=1, ignore_index=False))
        return calculated_and_concated_with_primary_df

    @staticmethod
    def execute_db_query(query, type_of_query):
        #print('In query execution')
        #print(query)
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')
        if type_of_query == 'select':
            print('In SELECT query execution')
            with engine.begin() as connection:
                df = pd.read_sql(query, con=connection)
            return df
        if type_of_query == 'truncate' or type_of_query == 'update' or type_of_query == 'insert':
            print('In TRUNCATE or UPDATE or INSERT query execution')
            with engine.begin() as connection:
                query = text(query)
                #print(query)
                connection.execute(query)


