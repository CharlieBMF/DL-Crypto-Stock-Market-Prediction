import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.layers import Conv1D, LSTM, Dense, Dropout, Bidirectional, TimeDistributed
from tensorflow.keras.layers import MaxPooling1D, Flatten
from tensorflow.keras.models import Sequential
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
pd.options.mode.chained_assignment = None

TABLES = ["BTC_KLINES_MONTHLY_12_2023_PER_3m"]

engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')
df = pd.DataFrame()
for table in TABLES:
    with engine.begin() as connection:
        query = "SELECT * FROM \"" + table + "\" ORDER BY \"ISOTimestampKlineCLOSE\" ASC"
        df1 = pd.read_sql(query, con=connection)

    df = pd.concat([df, df1], axis=0, ignore_index=True)
    print(table, len(df))


df.drop(columns=['ISOInsertTimestamp', 'ISOTimestampKlineCLOSE', 'UNIXTimestampKlineOPEN', 'UNIXTimestampKlineCLOSE',
                 'close_minus99_d', 'close_80_roc', 'close_99_roc', 'close_150_roc', 'mfi_80', 'mfi_99', 'mfi_150',
                 'ftr_80', 'ftr_99', 'ftr_150', ],
        inplace=True)
print(df)



X = []
Y = []
window_size = 100
for i in range(1, len(df) - window_size - 1, 1):

    first = df.iloc[i, 3]
    print('first', first)
    temp = []
    temp2 = []
    for j in range(window_size):
        temp.append((df.iloc[i + j, 3] - first) / first)
    temp2.append((df.iloc[i + window_size, 3] - first) / first)
    X.append(np.array(temp).reshape(100, 1))
    Y.append(np.array(temp2).reshape(1, 1))

x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, shuffle=True)

train_X = np.array(x_train)
test_X = np.array(x_test)
train_Y = np.array(y_train)
test_Y = np.array(y_test)

train_X = train_X.reshape(train_X.shape[0], 1, 100, 1)
test_X = test_X.reshape(test_X.shape[0], 1, 100, 1)

print(train_X)
print(train_Y)

model = tf.keras.Sequential()

# Creating the Neural Network model here...
# CNN layers
model.add(TimeDistributed(Conv1D(64, kernel_size=3, activation='relu', input_shape=(None, 100, 1))))
model.add(TimeDistributed(MaxPooling1D(2)))
model.add(TimeDistributed(Conv1D(128, kernel_size=3, activation='relu')))
model.add(TimeDistributed(MaxPooling1D(2)))
model.add(TimeDistributed(Conv1D(64, kernel_size=3, activation='relu')))
model.add(TimeDistributed(MaxPooling1D(2)))
model.add(TimeDistributed(Flatten()))
# model.add(Dense(5, kernel_regularizer=L2(0.01)))

# LSTM layers
model.add(Bidirectional(LSTM(100, return_sequences=True)))
model.add(Dropout(0.5))
model.add(Bidirectional(LSTM(100, return_sequences=False)))
model.add(Dropout(0.5))

#Final layers
model.add(Dense(1, activation='linear'))
model.compile(optimizer='adam', loss='mse', metrics=['mse', 'mae'])

history = model.fit(train_X, train_Y, validation_data=(test_X, test_Y), epochs=10,batch_size=64, verbose=1, shuffle =True)
print(history)

















TABLES_test = ["BTC_KLINES_MONTHLY_1_2024_PER_3m"]
engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')
df_test = pd.DataFrame()
for table in TABLES_test:
    with engine.begin() as connection:
        query = "SELECT * FROM \"" + table + "\" ORDER BY \"ISOTimestampKlineCLOSE\" ASC"
        df2 = pd.read_sql(query, con=connection)

    df_test = pd.concat([df_test, df2], axis=0, ignore_index=True)
    print(table, len(df))


df_test.drop(columns=['ISOInsertTimestamp', 'ISOTimestampKlineCLOSE', 'UNIXTimestampKlineOPEN', 'UNIXTimestampKlineCLOSE',
                 'close_minus99_d', 'close_80_roc', 'close_99_roc', 'close_150_roc', 'mfi_80', 'mfi_99', 'mfi_150',
                 'ftr_80', 'ftr_99', 'ftr_150', ],
        inplace=True)
print(df_test)