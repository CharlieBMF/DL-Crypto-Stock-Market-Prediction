import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM
import math
from sklearn.metrics import mean_squared_error

df = pd.read_excel("stock_stats.xlsx")
df.drop(columns=['ISOInsertTimestamp', 'ISOTimestampKlineCLOSE', 'UNIXTimestampKlineOPEN', 'UNIXTimestampKlineCLOSE',
                 'close_minus99_d', 'close_80_roc', 'close_99_roc', 'close_150_roc', 'mfi_80', 'mfi_99', 'mfi_150',
                 'ftr_80', 'ftr_99', 'ftr_150', ],
        inplace=True)
print(df)

df2 = df.reset_index()['closePrice']
plt.plot(df2)
plt.show()


scaler = MinMaxScaler()
df2 = scaler.fit_transform(np.array(df2).reshape(-1,1))
print(df2.shape)

train_size = int(len(df2) * 0.8)
test_size = len(df2 - train_size)
train_data, test_data = df2[0:train_size,:], df2[train_size:len(df2),:1]


def create_dataset(dataset, time_step = 1):
    dataX, dataY = [], []
    for i in range(len(dataset)-time_step-1):
        a = dataset[i:(i+time_step), 0]
        dataX.append(a)
        dataY.append(dataset[i + time_step, 0])
    return np.array(dataX), np.array(dataY)


timestep = 60
X_train, Y_train = create_dataset(train_data, timestep)
X_test, Y_test = create_dataset(test_data, timestep)


model = Sequential()
model.add(LSTM(50, return_sequences=True, input_shape=(X_train.shape[1],1)))
model.add(LSTM(50, return_sequences=True))
model.add(LSTM(50))
model.add(Dense(1))
model.compile(loss='mean_squared_error', optimizer='adam')

model.summary()

model.fit(X_train, Y_train, validation_data=(X_test, Y_test), epochs=100, batch_size=64, verbose=1)

train_predict = model.predict(X_train)

test_predict = model.predict(X_test)

train_predict = scaler.inverse_transform(train_predict)
print('TRAIN PREDICT', train_predict)
print('len TRAIN PREDICT', len(train_predict))
test_predict = scaler.inverse_transform(test_predict)

print(math.sqrt(mean_squared_error(Y_train, train_predict)))
print(math.sqrt(mean_squared_error(Y_test, test_predict)))


look_back = 60

train_predict_plot = np.empty_like(df2)
train_predict_plot[:, :] = np.nan
train_predict_plot[look_back : len(train_predict) + look_back,:] = train_predict

test_predict_plot = np.empty_like(df2)
test_predict_plot[:, :] = np.nan
test_predict_plot[len(train_predict) + (look_back) * 2 + 1: len(df2) - 1, :] = test_predict

plt.plot(scaler.inverse_transform(df2))
plt.plot(train_predict_plot)
plt.plot(test_predict_plot)
plt.show()

