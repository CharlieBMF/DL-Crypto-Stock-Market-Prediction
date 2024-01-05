import time
from sqlalchemy import create_engine, text
import requests
import urllib.parse
import pandas as pd
from datetime import datetime
import json


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
        print(response.text)
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

    def __init__(self, base_url, db_connection_string):
        self.server_time = 1503273600000  # 21.08.2017
        self.db_connection_string = db_connection_string
        self.next_table_name = ''
        self.table_creation_rules = {
            'never': ['1w', '3d', '1d'], 'yearly': ['12h', '8h', '6h', '4h', '2h', '1h'],
            'monthly': ['30m', '15m', '5m', '3m', '1m']
        }
        self.possible_periods = ['1w', '3d', '1d', '12h', '8h', '6h', '4h', '2h', '1h', '30m', '15m', '5m', '3m', '1m']
        self.actual_period = '15m'
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
            '1m': '', '3m': '', '5m': '','15m': '', '30m': '', '1h': '', '2h': '', '4h': '', '6h': '', '8h': '',
            '12h': '', '1d': '', '3d': '', '1w': '', #'1M': ''
        }
        self.next_table_name = {
            '1m': '', '3m': '', '5m': '','15m': '', '30m': '', '1h': '', '2h': '', '4h': '', '6h': '', '8h': '',
            '12h': '', '1d': '', '3d': '', '1w': '', #'1M': ''
        }
        self.existed_tables = {
            '1m': '', '3m': '', '5m': '','15m': '', '30m': '', '1h': '', '2h': '', '4h': '', '6h': '', '8h': '',
            '12h': '', '1d': '', '3d': '', '1w': '', #'1M': ''
        }
        self.sql_main_columns = [
            'ISOInsertTimestamp', 'ISOTimestampKlineOPEN', 'UNIXInsertTimestamp', 'UNIXTimestampKlineOPEN',
            'UNIXTimestampKlineCLOSE', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'quoteAssetVolume',
            'tradesAmount', 'takerButBase', 'takerBuyQuote'
        ]
        self.cached_last_150_rows = pd.DataFrame()
        super().__init__(base_url)

    def update_server_time(self):
        server_response = super().check_time()
        self.server_time = int(server_response.json()['serverTime'])
        print('self server time', self.server_time)

    def get_data_from_sql_checkpoints(self):
        query = text("SELECT * FROM \"_CHECKPOINTS\"")
        checkpoints = self.execute_db_query(query)
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
        query = "SELECT * FROM \"BTC_KLINES_CACHE_" + self.actual_period + "\""
        self.cached_last_150_rows = self.execute_db_query(query)
        print(self.cached_last_150_rows)

    def check_necessary_to_create_a_table(self):
        data = self.get_self_requests_years_and_months()
        print('data', data)
#        if self.actual_period in self.table_creation_rules['yearly']:
#            if data['next_request_year'] > data['last_request_year']:
#                return True
#            return True
        print(self.actual_period, type(self.actual_period))
        if self.actual_period in self.table_creation_rules['monthly']:
#            if (data['next_request_year'] > data['last_request_year'] or
#                    data['next_request_month'] > data['last_request_month']):
#                return True
            return True
        return False

    def create_table_for_period(self):
        query = ("CREATE TABLE \"" + self.next_table_name[self.actual_period] +
                 "\" AS SELECT * FROM \"" + self.actual_table_name[self.actual_period] + "\" WITH NO DATA")
        print(query)
        #self.execute_db_query(query)

    def update_kline_settings(self, type_of_update):
        if type_of_update == 'tables':
            self.actual_table_name[self.actual_period] = self.next_table_name[self.actual_period]
            data = self.get_self_requests_years_and_months()
            if self.actual_period in self.table_creation_rules['yearly']:
                self.next_table_name[self.actual_period] = 'BTC_KLINES_YEARLY_' + data['next_request_year'] + '_PER_' + self.actual_period
            elif self.actual_period in self.table_creation_rules['monthly']:
                self.next_table_name[self.actual_period] = ('BTC_KLINES_MONTHLY_' + data['next_request_month'] + '_' +
                                                            data['next_request_year'] + '_PER_' + self.actual_period)
            self.existed_tables[self.actual_period].append(self.next_table_name[self.actual_period])
 #           print('generated next table name: ', self.next_table_name[self.actual_period])
 #           print('existed tables', self.existed_tables[self.actual_period])

    def continue_actual_period(self):
        if (self.last_request_timestamp[self.actual_period] + self.time_intervals_in_unix[self.actual_period]
                > self.server_time):
            print(f'{self.last_request_timestamp[self.actual_period]} + '
                  f'{self.time_intervals_in_unix[self.actual_period]} > '
                  f'{self.server_time})')
            return False
        return True

    def get_self_requests_years_and_months(self):
        last_request_year = datetime.utcfromtimestamp(self.last_request_timestamp[self.actual_period] / 1000).year
        last_request_month = datetime.utcfromtimestamp(self.last_request_timestamp[self.actual_period] / 1000).month
        next_request_year = datetime.utcfromtimestamp(self.next_request_timestamp[self.actual_period] / 1000).year
        next_request_month = datetime.utcfromtimestamp(self.next_request_timestamp[self.actual_period] / 1000).month
        return {
            'last_request_year': str(last_request_year), 'last_request_month': str(last_request_month),
            'next_request_year': str(next_request_year), 'next_request_month': str(next_request_month),
        }
    @staticmethod
    def execute_db_query(query):
        print('in query execution')
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')
        with engine.begin() as connection:
            print(query)
            df = pd.read_sql(query, con=connection)
            print(f'Response for {query} is \n {df}')
        return df

