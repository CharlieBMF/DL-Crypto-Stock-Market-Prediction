import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from base import Api
from orderbook_functions import fill_bids_value_to_range, fill_asks_value_to_range


binance = Api(base_url='https://api.binance.com')
orderbook_df = pd.DataFrame(columns=['Actual Price', 'Avg Price last 5 mins', 'Total Bid Quantity',
                                     'Total Asks Quantity',
                                     'b0-0.001', 'b0.001-0.002', 'b0.002-0.003', 'b0.003-0.004', 'b0.004-0.005',
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
                                     'a0-0.001', 'a0.001-0.002', 'a0.002-0.003', 'a0.003-0.004', 'a0.004-0.005',
                                     'a0.005-0.006', 'a0.006-0.007', 'a0.007-0.008', 'a0.008-0.009', 'a0.009-0.01',
                                     'a0.01-0.015', 'a0.015-0.02', 'a0.02-0.025', 'a0.025-0.03', 'a0.03-0.035',
                                     'a0.035-0.04', 'a0.04-0.045', 'a0.045-0.05', 'a0.05-0.055', 'a0.055-0.06',
                                     'a0.06-0.065',
                                     'a0.065-0.07', 'a0.07-0.075', 'a0.075-0.08', 'a0.08-0.085', 'a0.085-0.09',
                                     'a0.09-0.095', 'a0.095-0.1',
                                     'a0.1-0.12', 'a0.12-0.15', 'a0.15-0.2', 'a0.2-0.25',
                                     'a0.25-0.3', 'a0.3-0.35', 'a0.35-0.4', 'a0.4-0.45', 'a0.45-0.5', 'a0.5-0.6',
                                     'a0.6-0.7',
                                     'a0.7-0.8', 'a0.8-0.9', 'a0.9-1', 'a1-1+', 'a0-0.002', 'a0.002-0.004',
                                     'a0.004-0.006',
                                     'a0.006-0.008', 'a0.008-0.01', 'a0.01-0.02', 'a0.02-0.03', 'a0.03-0.04',
                                     'a0.04-0.05', 'a0.05-0.06', 'a0.06-0.07', 'a0.07-0.08', 'a0.08-0.09', 'a0.1-0.2',
                                     'a0.2-0.3',
                                     'a0.3-0.4', 'a0.4-0.5', 'a0.5-0.7', 'a0.7-1', 'a0-0.005', 'a0.005-0.01',
                                     'a0.01-0.05', 'a0.05-0.1', 'a0.1-0.3', 'a0.3-0.5', 'a0.5-1',
                                     ])


for i in range(0, 2):
    r = binance.get_order_book('BTCUSDT', limit='5000')
    order_book_data = r.json()
    order_book_data_bids = r.json()["bids"]
    order_book_data_asks = r.json()["asks"]

    orderbook_df.loc[len(orderbook_df)] = [0 for i in range(len(orderbook_df.columns))]

    r = binance.get_actual_price_ticker(['BTCUSDT'])
    actual_price = r.json()[0]['price']
    print(f'Actual price: {actual_price}')

    orderbook_df.at[len(orderbook_df) - 1, 'Actual Price'] = actual_price

    r = binance.get_avg_price('BTCUSDT')
    print(r)
    avg_price = r.json()['price']
    print(f'Average price: {avg_price}')
    orderbook_df.at[len(orderbook_df) - 1, 'Avg Price last 5 mins'] = avg_price

    for bid in order_book_data_bids:
        orderbook_df.at[len(orderbook_df)-1, 'Total Bid Quantity'] = (
                orderbook_df.iloc[len(orderbook_df)-1]['Total Bid Quantity'] + float(bid[1])
        )
        orderbook_percent = float(actual_price)/float(bid[0])*100 - 100
        orderbook_df = fill_bids_value_to_range(bid, orderbook_df, orderbook_percent)

    for ask in order_book_data_asks:
        orderbook_df.at[len(orderbook_df) - 1, 'Total Asks Quantity'] = (
                orderbook_df.iloc[len(orderbook_df) - 1]['Total Asks Quantity'] + float(ask[1])
        )
        orderbook_percent = float(ask[0])/float(actual_price)*100 - 100
        orderbook_df = fill_asks_value_to_range(ask, orderbook_df, orderbook_percent)


with pd.ExcelWriter(r'OrdBookBids.xlsx') as writer:
    orderbook_df.to_excel(writer)

czas = np.array([i for i in range(0, len(orderbook_df))])
print('Czas:', czas)
kolumny_df = ['b0-0.005', 'b0.005-0.01', 'b0.01-0.05', 'b0.05-0.1', 'b0.1-0.3', 'b0.3-0.5', 'b0.5-1', 'b1-1+', 'a0-0.005', 'a0.005-0.01', 'a0.01-0.05', 'a0.05-0.1', 'a0.1-0.3', 'a0.3-0.5', 'a0.5-1', 'a1-1+']
mnozniki_w_kolumnach_df = [0.0025/100, 0.0075/100, 0.025/100, 0.075/100, 0.2/100, 0.4/100, 0.75/100, 2/100, -0.0025/100, -0.0075/100, -0.025/100, -0.075/100, -0.2/100, -0.4/100, -0.75/100, -2/100]
polaczone = list(zip(kolumny_df, mnozniki_w_kolumnach_df))
print(polaczone)
print(float(orderbook_df.iloc[1]['Actual Price']) - float(orderbook_df.iloc[1]['Actual Price']))
ceny = [[float(orderbook_df.iloc[i]['Actual Price']) - float(orderbook_df.iloc[1]['Actual Price']) * mnoznik
       for mnoznik in mnozniki_w_kolumnach_df] for i in range(0, len(orderbook_df))]
print(ceny)

ilosci_btc_kupno = [[orderbook_df.iloc[i][kolumny] for kolumny in kolumny_df] for i in range(0, len(orderbook_df))]
actual_ceny = [float(orderbook_df.iloc[i]['Actual Price']) for i in range(0, len(orderbook_df))]
print(ilosci_btc_kupno)
for i in range(len(czas)):
    for j in range(len(ceny[i])):
        plt.scatter(czas[i], ceny[i][j], s=ilosci_btc_kupno[i][j], alpha=0.5)

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