import requests
import urllib.parse


class Api:

    def __init__(self, base_url):
        self.base_url = base_url
        self.avg_price_url = '/api/v3/avgPrice'
        self.order_book_url = 'api/v3/depth'
        self.recent_trades_url = 'api/v3/trades'
        self.historical_trades_url = 'api/v3/historicalTrades'
        self.klines_url = '/api/v3/klines'
        self.last24hr_url = '/api/v3/ticker/24hr'
        self.actual_price_ticker_url = '/api/v3/ticker/price'
        self.order_book_ticker_url = '/api/v3/ticker/bookTicker'
        self.price_change_ticker_url = '/api/v3/ticker'

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
                    "time": 1499865549590, // Trade executed timestamp, as same as `T` in the stream
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

    def get_klines_data(self, symbol, interval, limit='500'):
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
        limit_url = '&limit=' + limit
        url = urllib.parse.urljoin(self.base_url, self.klines_url) + symbol_url + interval_url + limit_url
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