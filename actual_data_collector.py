import posixpath
import requests
import os
import urllib.parse
import websockets
import asyncio
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


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
        print(response.text)
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

binance = Api(base_url='https://api.binance.com')
orderbook_df = pd.DataFrame(columns=['b0-0.001', 'b0.001-0.002', 'b0.002-0.003', 'b0.003-0.004', 'b0.004-0.005',
                                     'b0.005-0.006', 'b0.006-0.007', 'b0.007-0.008', 'b0.008-0.009', 'b0.009-0.01',
                                     'b0.01-0.015', 'b0.015-0.02', 'b0.02-0.025', 'b0.025-0.03', 'b0.03-0.035',
                                     'b0.035-0.04', 'b0.04-0.045', 'b0.045-0.05', 'b0.05-0.055', 'b0.055-0.06',
                                     'b0.06-0.065',
                                     'b0.065-0.07', 'b0.07-0.075', 'b0.075-0.08', 'b0.08-0.085', 'b0.085-0.09',
                                     'b0.09-0.095', 'b0.095-0.1',
                                     'b0.1-0.12', 'b0.12-0.15', 'b0.15-0.2', 'b0.2-0.25',
                                     'b0.25-0.3', 'b0.3-0.35', 'b0.35-0.4', 'b0.4-0.45', 'b0.45-0.5', 'b0.5-0.6',
                                     'b0.6-0.7',
                                     'b0.7-0.8', 'b0.8-0.9', 'b0.9-1', 'b1-1+', 'b0-0.002', 'b0.002-0.004',
                                     'b0.004-0.006',
                                     'b0.006-0.008', 'b0.008-0.01', 'b0.01-0.02', 'b0.02-0.03', 'b0.03-0.04',
                                     'b0.04-0.05', 'b0.05-0.06', 'b0.06-0.07', 'b0.07-0.08', 'b0.08-0.09', 'b0.1-0.2',
                                     'b0.2-0.3',
                                     'b0.3-0.4', 'b0.4-0.5', 'b0.5-0.7', 'b0.7-1', 'b0-0.005', 'b0.005-0.01',
                                     'b0.01-0.05', 'b0.05-0.1', 'b0.1-0.3', 'b0.3-0.5', 'b0.5-1',
                                     'Total Bid Quantity', 'Actual Price',
                                     'a0-0.001', 'a0.001-0.002', 'a0.002-0.003', 'a0.003-0.004', 'a0.004-0.005',
                                     'a0.005-0.006', 'a0.006-0.007', 'a0.007-0.008', 'a0.008-0.009', 'a0.009-0.01',
                                     'a0.01-0.015', 'a0.015-0.02', 'a0.02-0.025', 'a0.25-0.03', 'a0.03-0.035',
                                     'a0.035-0.04', 'a0.045-0.05', 'a0.05-0.06', 'a0.06-0.07', 'a0.07-0.08',
                                     'a0.08-0.09', 'a0.09-0.1', 'a0.1-0.12', 'a0.12-0.15', 'a0.15-0.2', 'a0.2-0.25',
                                     'a0.25-0.3', 'a0.3-0.35', 'a0.35-0.4', 'a0.45-0.5', 'a0.5-0.6', 'a0.6-0.7',
                                     'a0.7-0.8', 'a0.8-0.9', 'a0.9-1', 'a1-1+', 'a0.00-0.002', 'a0.002-0.004',
                                     'a0.004-0.006',
                                     'a0.006-0.008', 'a0.008-0.01', 'a0.01-0.02', 'a0.02-0.03', 'a0.03-0.04',
                                     'a0.04-0.05', 'a0.06-0.07', 'a0.07-0.08', 'a0.08-0.09', 'a0.1-0.2', 'a0.2-0.3',
                                     'a0.3-0.4', 'a0.4-0.5', 'a0.5-0.7', 'a0.7-1', 'a0.00-0.005', 'a0.005-0.01',
                                     'a0.01-0.05', 'a0.05-0.1', 'a0.1-0.3', 'a0.3-0.5', 'a0.5-1',
                                     'Total Asks Quantity'
                                     ])




# # Dane przykładowe
# czas = np.array([1, 2, 3, 4, 5])  # Czas
# ceny = np.array([[10, 12, 15], [17, 14, 20], [18, 19, 22], [21, 25, 16], [23, 24, 27]])  # Ceny dla ofert kupna
# ilosci_ofert_kupna = np.array([[5, 3, 8], [10, 6, 4], [7, 5, 9], [12, 6, 8], [9, 7, 11]])  # Ilości ofert kupna
#
# # Utworzenie wykresu punktowego z bąbelkami dla ofert kupna
# for i in range(len(czas)):
#     for j in range(len(ceny[i])):
#         plt.scatter(czas[i], ceny[i][j], s=ilosci_ofert_kupna[i][j]*10, alpha=0.5)
#
# # Utworzenie liniowego wykresu dla głównej ceny produktu
# plt.plot(czas, np.mean(ceny, axis=1), color='blue', label='Główna cena')
#
# # Konfiguracja osi i etykiet
# plt.xlabel('Czas')
# plt.ylabel('Cena')
# plt.legend()
#
# # Wyświetlenie wykresu
# plt.show()




for i in range(0, 20):
    r = binance.get_order_book('BTCUSDT', limit='5000')
    order_book_data = r.json()
    order_book_data_bids = r.json()["bids"]
    order_book_data_asks = r.json()["asks"]
    #print(f'Bids: {order_book_data_bids} \n Asks: {order_book_data_asks} \n')
    # order_book_bids_df = pd.DataFrame(columns=['bid price', 'bid qty'])
    # for bid in order_book_data_bids:
    #     order_book_bids_df = order_book_bids_df.append(pd.Series(bid, index=order_book_bids_df.columns), ignore_index=True)
    # print(order_book_bids_df)

    orderbook_df.loc[len(orderbook_df)] = [0 for i in range(len(orderbook_df.columns))]
    print(orderbook_df)
    #print(orderbook_df.iloc[len(orderbook_df)-1]['b0-0.001'])

    r = binance.get_actual_price_ticker(['BTCUSDT'])
    actual_price = r.json()[0]['price']
    print(f'Actual price: {actual_price}')

    for bid in order_book_data_bids:
        orderbook_percent = float(actual_price)/float(bid[0])*100 - 100
        #print(orderbook_percent, bid[1])
        if orderbook_percent < 0.001:
            orderbook_df.at[len(orderbook_df)-1, 'b0-0.001'] = orderbook_df.iloc[len(orderbook_df)-1]['b0-0.001'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0-0.002'] = orderbook_df.iloc[len(orderbook_df)-1]['b0-0.002'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df)-1]['b0-0.005'] + float(bid[1])
        elif 0.001 <= orderbook_percent < 0.002:
            orderbook_df.at[len(orderbook_df)-1, 'b0.001-0.002'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.001-0.002'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0-0.002'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0-0.002'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0-0.005'] + float(bid[1])
        elif 0.002 <= orderbook_percent < 0.003:
            orderbook_df.at[len(orderbook_df)-1, 'b0.002-0.003'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.002-0.003'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.002-0.004'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.002-0.004'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0-0.005'] + float(bid[1])
        elif 0.003 <= orderbook_percent < 0.004:
            orderbook_df.at[len(orderbook_df)-1, 'b0.003-0.004'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.003-0.004'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.002-0.004'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.002-0.004'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0-0.005'] + float(bid[1])
        elif 0.004 <= orderbook_percent < 0.005:
            orderbook_df.at[len(orderbook_df)-1, 'b0.004-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.001-0.002'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.004-0.006'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.004-0.006'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0-0.005'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0-0.005'] + float(bid[1])
        elif 0.005 <= orderbook_percent < 0.006:
            orderbook_df.at[len(orderbook_df)-1, 'b0.005-0.006'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.001-0.002'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.004-0.006'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.004-0.006'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.005-0.01'] + float(bid[1])
        elif 0.006 <= orderbook_percent < 0.007:
            orderbook_df.at[len(orderbook_df)-1, 'b0.006-0.007'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.006-0.007'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.006-0.008'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.006-0.008'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.005-0.01'] + float(bid[1])
        elif 0.007 <= orderbook_percent < 0.008:
            orderbook_df.at[len(orderbook_df)-1, 'b0.007-0.008'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.007-0.008'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.006-0.008'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.006-0.008'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.005-0.01'] + float(bid[1])
        elif 0.008 <= orderbook_percent < 0.009:
            orderbook_df.at[len(orderbook_df)-1, 'b0.008-0.009'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.008-0.009'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.008-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.008-0.01'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.005-0.01'] + float(bid[1])
        elif 0.009 <= orderbook_percent < 0.01:
            orderbook_df.at[len(orderbook_df)-1, 'b0.009-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.009-0.01'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.008-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.008-0.01'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.005-0.01'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.005-0.01'] + float(bid[1])
        elif 0.01 <= orderbook_percent < 0.015:
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.015'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.015'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.02'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.02'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.05'] + float(bid[1])
        elif 0.015 <= orderbook_percent < 0.02:
            orderbook_df.at[len(orderbook_df)-1, 'b0.015-0.02'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.015-0.02'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.02'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.02'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.05'] + float(bid[1])
        elif 0.02 <= orderbook_percent < 0.025:
            orderbook_df.at[len(orderbook_df)-1, 'b0.02-0.025'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.02-0.025'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.02-0.03'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.02-0.03'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.05'] + float(bid[1])
        elif 0.025 <= orderbook_percent < 0.03:
            orderbook_df.at[len(orderbook_df)-1, 'b0.025-0.03'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.025-0.03'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.02-0.03'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.02-0.03'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.05'] + float(bid[1])
        elif 0.03 <= orderbook_percent < 0.035:
            orderbook_df.at[len(orderbook_df)-1, 'b0.03-0.035'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.03-0.035'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.03-0.04'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.03-0.04'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.05'] + float(bid[1])
        elif 0.035 <= orderbook_percent < 0.04:
            orderbook_df.at[len(orderbook_df)-1, 'b0.035-0.04'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.035-0.04'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.03-0.04'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.03-0.04'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.05'] + float(bid[1])
        elif 0.04 <= orderbook_percent < 0.045:
            orderbook_df.at[len(orderbook_df)-1, 'b0.04-0.045'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.04-0.045'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.04-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.04-0.05'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.05'] + float(bid[1])
        elif 0.045 <= orderbook_percent < 0.05:
            orderbook_df.at[len(orderbook_df)-1, 'b0.045-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.045-0.05'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.04-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.04-0.05'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.01-0.05'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.01-0.05'] + float(bid[1])
        elif 0.05 <= orderbook_percent < 0.055:
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.055'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.055'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.06'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.06'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.1'] + float(bid[1])
        elif 0.055 <= orderbook_percent < 0.06:
            orderbook_df.at[len(orderbook_df)-1, 'b0.055-0.06'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.055-0.06'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.06'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.06'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.1'] + float(bid[1])
        elif 0.06 <= orderbook_percent < 0.065:
            orderbook_df.at[len(orderbook_df)-1, 'b0.06-0.065'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.06-0.065'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.06-0.07'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.06-0.07'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.1'] + float(bid[1])
        elif 0.065 <= orderbook_percent < 0.07:
            orderbook_df.at[len(orderbook_df)-1, 'b0.065-0.07'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.065-0.07'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.06-0.07'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.06-0.07'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.1'] + float(bid[1])
        elif 0.07 <= orderbook_percent < 0.075:
            orderbook_df.at[len(orderbook_df)-1, 'b0.07-0.075'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.07-0.075'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.07-0.08'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.07-0.08'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.1'] + float(bid[1])
        elif 0.075 <= orderbook_percent < 0.08:
            orderbook_df.at[len(orderbook_df)-1, 'b0.075-0.08'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.075-0.08'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.07-0.08'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.07-0.08'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.1'] + float(bid[1])
        elif 0.08 <= orderbook_percent < 0.085:
            orderbook_df.at[len(orderbook_df)-1, 'b0.08-0.085'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.08-0.085'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.08-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.08-0.09'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.1'] + float(bid[1])
        elif 0.085 <= orderbook_percent < 0.09:
            orderbook_df.at[len(orderbook_df)-1, 'b0.085-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.085-0.09'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.08-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.08-0.09'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.1'] + float(bid[1])
        elif 0.09 <= orderbook_percent < 0.095:
            orderbook_df.at[len(orderbook_df)-1, 'b0.09-0.095'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.09-0.095'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.08-0.09'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.08-0.09'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.05-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.05-0.1'] + float(bid[1])
        elif 0.095 <= orderbook_percent < 0.1:
            orderbook_df.at[len(orderbook_df)-1, 'b0.095-0.1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.095-0.1'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.2'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.3'] + float(bid[1])
        elif 0.1 <= orderbook_percent < 0.12:
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.12'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.12'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.2'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.3'] + float(bid[1])
        elif 0.12 <= orderbook_percent < 0.15:
            orderbook_df.at[len(orderbook_df)-1, 'b0.12-0.15'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.12-0.15'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.2'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.3'] + float(bid[1])
        elif 0.15 <= orderbook_percent < 0.2:
            orderbook_df.at[len(orderbook_df)-1, 'b0.15-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.15-0.2'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.2'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.2'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.3'] + float(bid[1])
        elif 0.2 <= orderbook_percent < 0.25:
            orderbook_df.at[len(orderbook_df)-1, 'b0.2-0.25'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.2-0.25'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.2-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.2-0.3'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.3'] + float(bid[1])
        elif 0.25 <= orderbook_percent < 0.3:
            orderbook_df.at[len(orderbook_df)-1, 'b0.25-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.25-0.3'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.2-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.2-0.3'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.1-0.3'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.1-0.3'] + float(bid[1])
        elif 0.3 <= orderbook_percent < 0.35:
            orderbook_df.at[len(orderbook_df)-1, 'b0.3-0.35'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.3-0.35'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.3-0.4'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.3-0.4'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.3-0.5'] + float(bid[1])
        elif 0.35 <= orderbook_percent < 0.4:
            orderbook_df.at[len(orderbook_df)-1, 'b0.35-0.4'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.35-0.4'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.3-0.4'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.3-0.4'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.3-0.5'] + float(bid[1])
        elif 0.4 <= orderbook_percent < 0.45:
            orderbook_df.at[len(orderbook_df)-1, 'b0.4-0.45'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.4-0.45'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.4-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.4-0.5'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.3-0.5'] + float(bid[1])
        elif 0.45 <= orderbook_percent < 0.5:
            orderbook_df.at[len(orderbook_df)-1, 'b0.45-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.45-0.5'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.4-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.4-0.5'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.3-0.5'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.3-0.5'] + float(bid[1])
        elif 0.5 <= orderbook_percent < 0.6:
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-0.6'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-0.6'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-0.7'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(bid[1])
        elif 0.6 <= orderbook_percent < 0.7:
            orderbook_df.at[len(orderbook_df)-1, 'b0.6-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.6-0.7'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-0.7'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(bid[1])
        elif 0.7 <= orderbook_percent < 0.8:
            orderbook_df.at[len(orderbook_df)-1, 'b0.7-0.8'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.7-0.8'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-0.7'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-0.7'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(bid[1])
        elif 0.7 <= orderbook_percent < 0.8:
            orderbook_df.at[len(orderbook_df)-1, 'b0.7-0.8'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.7-0.8'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.7-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.7-1'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(bid[1])
        elif 0.8 <= orderbook_percent < 0.9:
            orderbook_df.at[len(orderbook_df)-1, 'b0.8-0.9'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.8-0.9'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.7-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.7-1'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(bid[1])
        elif 0.9 <= orderbook_percent < 1:
            orderbook_df.at[len(orderbook_df)-1, 'b0.9-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.9-1'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.7-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.7-1'] + float(bid[1])
            orderbook_df.at[len(orderbook_df)-1, 'b0.5-1'] = orderbook_df.iloc[len(orderbook_df) - 1]['b0.5-1'] + float(bid[1])
        elif orderbook_percent >= 1:
            orderbook_df.at[len(orderbook_df)-1, 'b1-1+'] = orderbook_df.iloc[len(orderbook_df) - 1]['b1-1+'] + float(bid[1])

        orderbook_df.at[len(orderbook_df)-1, 'Total Bid Quantity'] = orderbook_df.iloc[len(orderbook_df)-1]['Total Bid Quantity'] + float(bid[1])
        orderbook_df.at[len(orderbook_df) - 1, 'Actual Price'] = actual_price
    print(orderbook_df)

with pd.ExcelWriter(r'OrdBookBids.xlsx') as writer:
    orderbook_df.to_excel(writer)

czas = np.array([i for i in range(0, len(orderbook_df))])
print('Czas:', czas)
# ceny = np.array([[10, 12, 15], [17, 14, 20], [18, 19, 22], [21, 25, 16], [23, 24, 27]])  # Ceny dla ofert kupna
kolumny_df = ['b0-0.005', 'b0.005-0.01', 'b0.01-0.05', 'b0.05-0.1', 'b0.1-0.3', 'b0.3-0.5', 'b0.5-1', 'b1-1+']
mnozniki_w_kolumnach_df = [0.0025/100, 0.0075/100, 0.025/100, 0.075/100, 0.2/100, 0.4/100, 0.75/100, 2/100]
polaczone = list(zip(kolumny_df, mnozniki_w_kolumnach_df))
print(polaczone)
ceny = [[float(orderbook_df.iloc[i]['Actual Price']) - float(orderbook_df.iloc[1]['Actual Price']) * mnoznik
       for mnoznik in mnozniki_w_kolumnach_df] for i in range(0, len(orderbook_df))]
print(ceny)

ilosci_btc_kupno = [[orderbook_df.iloc[i][kolumny] for kolumny in kolumny_df] for i in range(0, len(orderbook_df))]
actual_ceny = [float(orderbook_df.iloc[i]['Actual Price']) for i in range(0, len(orderbook_df))]
print(ilosci_btc_kupno)
for i in range(len(czas)):
    for j in range(len(ceny[i])):
        plt.scatter(czas[i], ceny[i][j], s=ilosci_btc_kupno[i][j]*10, alpha=0.5)

plt.plot(czas, actual_ceny, color='blue', label='Główna cena')

# Konfiguracja osi i etykiet
plt.xlabel('Czas')
plt.ylabel('Cena')
plt.legend()

# Wyświetlenie wykresu
plt.show()

#ceny = np.array([cena for cena in [orderbook_df.iloc[1][kolumna] * mnoznik for (kolumna, mnoznik) in list(polaczone)]])
#print(ceny)
# ilosci_ofert_kupna = np.array([[5, 3, 8], [10, 6, 4], [7, 5, 9], [12, 6, 8], [9, 7, 11]])  # Ilości ofert kupna



# # Utworzenie wykresu punktowego z bąbelkami dla ofert kupna
# for i in range(len(czas)):
#     for j in range(len(ceny[i])):
#         plt.scatter(czas[i], ceny[i][j], s=ilosci_ofert_kupna[i][j]*10, alpha=0.5)
#
# # Utworzenie liniowego wykresu dla głównej ceny produktu
# plt.plot(czas, np.mean(ceny, axis=1), color='blue', label='Główna cena')
#
# # Konfiguracja osi i etykiet
# plt.xlabel('Czas')
# plt.ylabel('Cena')
# plt.legend()
#
# # Wyświetlenie wykresu
# plt.show()





# binance.get_recent_trade('BTCUSDT')
# binance.get_historical_trades('BTCUSDT')
# binance.get_klines_data('BTCUSDT', '1s')
# binance.get_last_24hr(['BTCUSDT'])
# binance.get_actual_price_ticker(["BTCUSDT"])
# binance.get_book_ticker(["BTCUSDT"])
# binance.get_price_change_statistic(["BTCUSDT"], "1h")












# jsosn={
#   "id": "51e2affb-0aba-4821-ba75-f2625006eb43",
#   "method": "depth",
#   "params": {
#     "symbol": "BNBBTC",
#     "limit": 5
#   }
# }
# async def test():
#     async with websockets.connect('wss://ws-api.binance.com:443/ws-api/v3') as websocket:
#         await websocket.send(json.dumps(jsosn))
#         respo = await websocket.recv()
#         print(respo)
#
# asyncio.get_event_loop().run_until_complete(test())