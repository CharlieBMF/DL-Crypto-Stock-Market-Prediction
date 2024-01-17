import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.layers import Dense, LSTM
from tensorflow.keras.models import Sequential
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
pd.options.mode.chained_assignment = None



#df = pd.read_excel("stock_stats.xlsx")


TABLES = ["BTC_KLINES_MONTHLY_8_2017_PER_3m", "BTC_KLINES_MONTHLY_9_2017_PER_3m", "BTC_KLINES_MONTHLY_10_2017_PER_3m", "BTC_KLINES_MONTHLY_11_2017_PER_3m", "BTC_KLINES_MONTHLY_12_2017_PER_3m", "BTC_KLINES_MONTHLY_1_2018_PER_3m", "BTC_KLINES_MONTHLY_2_2018_PER_3m", "BTC_KLINES_MONTHLY_3_2018_PER_3m", "BTC_KLINES_MONTHLY_4_2018_PER_3m", "BTC_KLINES_MONTHLY_5_2018_PER_3m", "BTC_KLINES_MONTHLY_6_2018_PER_3m", "BTC_KLINES_MONTHLY_7_2018_PER_3m", "BTC_KLINES_MONTHLY_8_2018_PER_3m", "BTC_KLINES_MONTHLY_9_2018_PER_3m", "BTC_KLINES_MONTHLY_10_2018_PER_3m", "BTC_KLINES_MONTHLY_11_2018_PER_3m", "BTC_KLINES_MONTHLY_12_2018_PER_3m", "BTC_KLINES_MONTHLY_1_2019_PER_3m", "BTC_KLINES_MONTHLY_2_2019_PER_3m", "BTC_KLINES_MONTHLY_3_2019_PER_3m", "BTC_KLINES_MONTHLY_4_2019_PER_3m", "BTC_KLINES_MONTHLY_5_2019_PER_3m", "BTC_KLINES_MONTHLY_6_2019_PER_3m", "BTC_KLINES_MONTHLY_7_2019_PER_3m", "BTC_KLINES_MONTHLY_8_2019_PER_3m", "BTC_KLINES_MONTHLY_9_2019_PER_3m", "BTC_KLINES_MONTHLY_10_2019_PER_3m", "BTC_KLINES_MONTHLY_11_2019_PER_3m", "BTC_KLINES_MONTHLY_12_2019_PER_3m", "BTC_KLINES_MONTHLY_1_2020_PER_3m", "BTC_KLINES_MONTHLY_2_2020_PER_3m", "BTC_KLINES_MONTHLY_3_2020_PER_3m", "BTC_KLINES_MONTHLY_4_2020_PER_3m", "BTC_KLINES_MONTHLY_5_2020_PER_3m", "BTC_KLINES_MONTHLY_6_2020_PER_3m", "BTC_KLINES_MONTHLY_7_2020_PER_3m", "BTC_KLINES_MONTHLY_8_2020_PER_3m", "BTC_KLINES_MONTHLY_9_2020_PER_3m", "BTC_KLINES_MONTHLY_10_2020_PER_3m", "BTC_KLINES_MONTHLY_11_2020_PER_3m", "BTC_KLINES_MONTHLY_12_2020_PER_3m", "BTC_KLINES_MONTHLY_1_2021_PER_3m", "BTC_KLINES_MONTHLY_2_2021_PER_3m", "BTC_KLINES_MONTHLY_3_2021_PER_3m", "BTC_KLINES_MONTHLY_4_2021_PER_3m", "BTC_KLINES_MONTHLY_5_2021_PER_3m", "BTC_KLINES_MONTHLY_6_2021_PER_3m", "BTC_KLINES_MONTHLY_7_2021_PER_3m", "BTC_KLINES_MONTHLY_8_2021_PER_3m", "BTC_KLINES_MONTHLY_9_2021_PER_3m", "BTC_KLINES_MONTHLY_10_2021_PER_3m", "BTC_KLINES_MONTHLY_11_2021_PER_3m", "BTC_KLINES_MONTHLY_12_2021_PER_3m", "BTC_KLINES_MONTHLY_1_2022_PER_3m", "BTC_KLINES_MONTHLY_2_2022_PER_3m", "BTC_KLINES_MONTHLY_3_2022_PER_3m", "BTC_KLINES_MONTHLY_4_2022_PER_3m", "BTC_KLINES_MONTHLY_5_2022_PER_3m", "BTC_KLINES_MONTHLY_6_2022_PER_3m", "BTC_KLINES_MONTHLY_7_2022_PER_3m", "BTC_KLINES_MONTHLY_8_2022_PER_3m", "BTC_KLINES_MONTHLY_9_2022_PER_3m", "BTC_KLINES_MONTHLY_10_2022_PER_3m", "BTC_KLINES_MONTHLY_11_2022_PER_3m", "BTC_KLINES_MONTHLY_12_2022_PER_3m", "BTC_KLINES_MONTHLY_1_2023_PER_3m", "BTC_KLINES_MONTHLY_2_2023_PER_3m", "BTC_KLINES_MONTHLY_3_2023_PER_3m", "BTC_KLINES_MONTHLY_4_2023_PER_3m", "BTC_KLINES_MONTHLY_5_2023_PER_3m", "BTC_KLINES_MONTHLY_6_2023_PER_3m", "BTC_KLINES_MONTHLY_7_2023_PER_3m", "BTC_KLINES_MONTHLY_8_2023_PER_3m", "BTC_KLINES_MONTHLY_9_2023_PER_3m", "BTC_KLINES_MONTHLY_10_2023_PER_3m", "BTC_KLINES_MONTHLY_11_2023_PER_3m", "BTC_KLINES_MONTHLY_12_2023_PER_3m"]
#TABLES =["BTC_KLINES_MONTHLY_1_2019_PER_3m", "BTC_KLINES_MONTHLY_2_2019_PER_3m", "BTC_KLINES_MONTHLY_3_2019_PER_3m", "BTC_KLINES_MONTHLY_4_2019_PER_3m", "BTC_KLINES_MONTHLY_5_2019_PER_3m", "BTC_KLINES_MONTHLY_6_2019_PER_3m", "BTC_KLINES_MONTHLY_7_2019_PER_3m", "BTC_KLINES_MONTHLY_8_2019_PER_3m", "BTC_KLINES_MONTHLY_9_2019_PER_3m", "BTC_KLINES_MONTHLY_10_2019_PER_3m", "BTC_KLINES_MONTHLY_11_2019_PER_3m", "BTC_KLINES_MONTHLY_12_2019_PER_3m", "BTC_KLINES_MONTHLY_1_2020_PER_3m", "BTC_KLINES_MONTHLY_2_2020_PER_3m", "BTC_KLINES_MONTHLY_3_2020_PER_3m", "BTC_KLINES_MONTHLY_4_2020_PER_3m", "BTC_KLINES_MONTHLY_5_2020_PER_3m", "BTC_KLINES_MONTHLY_6_2020_PER_3m", "BTC_KLINES_MONTHLY_7_2020_PER_3m", "BTC_KLINES_MONTHLY_8_2020_PER_3m", "BTC_KLINES_MONTHLY_9_2020_PER_3m", "BTC_KLINES_MONTHLY_10_2020_PER_3m", "BTC_KLINES_MONTHLY_11_2020_PER_3m", "BTC_KLINES_MONTHLY_12_2020_PER_3m", "BTC_KLINES_MONTHLY_1_2021_PER_3m", "BTC_KLINES_MONTHLY_2_2021_PER_3m", "BTC_KLINES_MONTHLY_3_2021_PER_3m", "BTC_KLINES_MONTHLY_4_2021_PER_3m", "BTC_KLINES_MONTHLY_5_2021_PER_3m", "BTC_KLINES_MONTHLY_6_2021_PER_3m", "BTC_KLINES_MONTHLY_7_2021_PER_3m", "BTC_KLINES_MONTHLY_8_2021_PER_3m", "BTC_KLINES_MONTHLY_9_2021_PER_3m", "BTC_KLINES_MONTHLY_10_2021_PER_3m", "BTC_KLINES_MONTHLY_11_2021_PER_3m", "BTC_KLINES_MONTHLY_12_2021_PER_3m", "BTC_KLINES_MONTHLY_1_2022_PER_3m", "BTC_KLINES_MONTHLY_2_2022_PER_3m", "BTC_KLINES_MONTHLY_3_2022_PER_3m", "BTC_KLINES_MONTHLY_4_2022_PER_3m", "BTC_KLINES_MONTHLY_5_2022_PER_3m", "BTC_KLINES_MONTHLY_6_2022_PER_3m", "BTC_KLINES_MONTHLY_7_2022_PER_3m", "BTC_KLINES_MONTHLY_8_2022_PER_3m", "BTC_KLINES_MONTHLY_9_2022_PER_3m", "BTC_KLINES_MONTHLY_10_2022_PER_3m", "BTC_KLINES_MONTHLY_11_2022_PER_3m", "BTC_KLINES_MONTHLY_12_2022_PER_3m", "BTC_KLINES_MONTHLY_1_2023_PER_3m", "BTC_KLINES_MONTHLY_2_2023_PER_3m", "BTC_KLINES_MONTHLY_3_2023_PER_3m", "BTC_KLINES_MONTHLY_4_2023_PER_3m", "BTC_KLINES_MONTHLY_5_2023_PER_3m", "BTC_KLINES_MONTHLY_6_2023_PER_3m", "BTC_KLINES_MONTHLY_7_2023_PER_3m", "BTC_KLINES_MONTHLY_8_2023_PER_3m", "BTC_KLINES_MONTHLY_9_2023_PER_3m", "BTC_KLINES_MONTHLY_10_2023_PER_3m", "BTC_KLINES_MONTHLY_11_2023_PER_3m", "BTC_KLINES_MONTHLY_12_2023_PER_3m"]

#TABLES = ["BTC_KLINES_MONTHLY_8_2017_PER_3m"]
engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')
df = pd.DataFrame()
for table in TABLES:
    with engine.begin() as connection:
        query = "SELECT * FROM \"" + table + "\""
        df1 = pd.read_sql(query, con=connection)

    df = pd.concat([df, df1], axis=0, ignore_index=True)
    print(table, len(df))


df.drop(columns=['ISOInsertTimestamp', 'ISOTimestampKlineCLOSE', 'UNIXTimestampKlineOPEN', 'UNIXTimestampKlineCLOSE',
                 'close_minus99_d', 'close_80_roc', 'close_99_roc', 'close_150_roc', 'mfi_80', 'mfi_99', 'mfi_150',
                 'ftr_80', 'ftr_99', 'ftr_150', ],
        inplace=True)
print(df)



y = df['closePrice'].fillna(method='ffill')
y = y.values.reshape(-1, 1)

scaler = MinMaxScaler(feature_range=(0, 1))
scaler = scaler.fit(y)
y = scaler.transform(y)

n_lookback = 250  # length of input sequences (lookback period)
n_forecast = 300  # length of output sequences (forecast period)

X = []
Y = []

for i in range(n_lookback, len(y) - n_forecast + 1):
    X.append(y[i - n_lookback: i])
    Y.append(y[i: i + n_forecast])

X = np.array(X)
Y = np.array(Y)

model = Sequential()
model.add(LSTM(units=50, return_sequences=True, input_shape=(n_lookback, 1)))
model.add(LSTM(units=50))
model.add(Dense(n_forecast))

model.compile(loss='mean_squared_error', optimizer='adam')
model.fit(X, Y, epochs=100, batch_size=32, verbose=1)

X_ = y[- n_lookback:]  # last available input sequence
X_ = X_.reshape(1, n_lookback, 1)

Y_ = model.predict(X_).reshape(-1, 1)
Y_ = scaler.inverse_transform(Y_)

# organize the results in a data frame
df_past = df[['closePrice']].reset_index()
df_past.rename(columns={'index': 'Date', 'closePrice': 'Actual'}, inplace=True)
df_past['Date'] = pd.to_datetime(df_past['Date'])
df_past['Forecast'] = np.nan
df_past['Forecast'].iloc[-1] = df_past['Actual'].iloc[-1]

df_future = pd.DataFrame(columns=['Date', 'Actual', 'Forecast'])
df_future['Date'] = pd.date_range(start=df_past['Date'].iloc[-1] + pd.Timedelta(days=1), periods=n_forecast)
df_future['Forecast'] = Y_.flatten()
df_future['Actual'] = np.nan

results = df_past._append(df_future, ignore_index=True)
print(results)


actual_plot = results['Actual']
future_plot = results['Forecast']

plt.plot(actual_plot)
plt.plot(future_plot)
plt.show()

with pd.ExcelWriter(r'optionC.xlsx') as writer:
    df_future.to_excel(writer)

model.save('per_1m_test.h5')