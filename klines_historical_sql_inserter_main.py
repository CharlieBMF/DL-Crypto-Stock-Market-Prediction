import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from base import Api, Klines
import time
import calendar
from datetime import datetime
from stockstats import wrap, unwrap
from sqlalchemy import create_engine, text
import threading
import signal


def handle_kb_interrupt(sig, frame):
    stop_event.set()


def preparation():
    klines = Klines(
        base_url='https://api.binance.com',
        db_connection_string='postgresql://postgres:postgres@127.0.0.1/CryptoData',
        )
    klines.get_data_from_sql_checkpoints()
    return klines


def loop(klines):
    df_for_stock_stats = pd.DataFrame(columns=['amount', 'close', 'high', 'low', 'volume'])
    for i in range(0, 1):

        klines.update_server_time()

        if not klines.continue_actual_period():
            print('Actual period not continued')
            klines.change_period()
            continue

        klines.get_data_from_sql_cache()

        if klines.check_necessary_to_create_a_table():
            klines.create_table_for_period()
            klines.update_kline_settings('tables')

        klines.get_and_save_new_klines()






        break
        klines.update_checkpoints_table_in_sql()
        if stop_event.is_set():
            break

#    engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')
#    query = "SELECT * FROM \"BTC_KLINES_CACHE_" + actual_period + "\""
#    print(query)
#    df_cache = pd.read_sql(text(query), con=engine)
#    engine.dispose()
#    print(df_cache)

    # engine = create_engine('postgresql://postgres:postgres@127.0.0.1/CryptoData')
    # with engine.begin() as connection:
    #     query = text("SELECT * FROM \"BTC_KLINES_CACHE_" + actual_period + "\"")
    #     print(query)
    #     df_cache = pd.read_sql(query, con=connection)
    #     print(df_cache)


if __name__ == '__main__':
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, handle_kb_interrupt)
    klines = preparation()
    thread = threading.Thread(target=loop, args=(klines,))
    thread.start()
    thread.join()
    print('Program done')
