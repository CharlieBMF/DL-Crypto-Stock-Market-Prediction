import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import keras
import tensorflow as tf
from keras.preprocessing.sequence import TimeseriesGenerator
from keras.models import Sequential
from keras.layers import LSTM, Dense
import plotly.graph_objs as go


df = pd.read_excel("stock_stats.xlsx")
#'ISOTimestampKlineCLOSE',
df.drop(columns=['ISOInsertTimestamp', 'UNIXTimestampKlineOPEN', 'UNIXTimestampKlineCLOSE',
                 'close_minus99_d', 'close_80_roc', 'close_99_roc', 'close_150_roc', 'mfi_80', 'mfi_99', 'mfi_150',
                 'ftr_80', 'ftr_99', 'ftr_150', ],
        inplace=True)
print(df)

df2 = df.reset_index()['closePrice']
plt.plot(df2)
plt.show()


close_data = df['closePrice'].values
close_data = close_data.reshape((-1, 1))

split_percent = 0.80
split = int(split_percent*len(close_data))

close_train = close_data[:split]
print(close_train)
close_test = close_data[split:]

date_train = df['ISOTimestampKlineCLOSE'][:split]
date_test = df['ISOTimestampKlineCLOSE'][split:]

look_back = 15
train_generator = TimeseriesGenerator(close_train, close_train, length=look_back, batch_size=20)
test_generator = TimeseriesGenerator(close_test, close_test, length=look_back, batch_size=1)


model = Sequential()
model.add(
    LSTM(10,
        activation='relu',
        input_shape=(look_back,1))
)
model.add(Dense(1))
model.compile(optimizer='adam', loss='mse')

model.fit_generator(train_generator, epochs=100, verbose=1)

prediction = model.predict_generator(test_generator)

close_train = close_train.reshape((-1))
close_test = close_test.reshape((-1))
prediction = prediction.reshape((-1))


close_data = close_data.reshape((-1))


def predict(num_prediction, model):
    prediction_list = close_data[-look_back:]
    print(prediction_list)

    for _ in range(num_prediction):
        x = prediction_list[-look_back:]
        x = x.reshape((1, look_back, 1))
        print(x)
        out = model.predict(x)[0][0]
        prediction_list = np.append(prediction_list, out)
    prediction_list = prediction_list[look_back - 1:]

    return prediction_list


def predict_dates(num_prediction):
    last_date = df['ISOTimestampKlineCLOSE'].values[-1]
    prediction_dates = pd.date_range(last_date, periods=num_prediction + 1).tolist()
    return prediction_dates


num_prediction = 100
forecast = predict(num_prediction, model)
forecast_dates = predict_dates(num_prediction)


trace1 = go.Scatter(
    x = date_train,
    y = close_train,
    mode = 'lines',
    name = 'Data'
)
trace2 = go.Scatter(
    x = date_test,
    y = prediction,
    mode = 'lines',
    name = 'Prediction'
)
trace3 = go.Scatter(
    x = date_test,
    y = close_test,
    mode='lines',
    name = 'Ground Truth'
)
trace4 = go.Scatter(
    x = forecast_dates,
    y = forecast,
    mode='lines',
    name = 'future'
)
layout = go.Layout(
    title = "Google Stock",
    xaxis = {'title' : "Date"},
    yaxis = {'title' : "Close"}
)
fig = go.Figure(data=[trace1, trace2, trace3, trace4], layout=layout)
fig.show()
