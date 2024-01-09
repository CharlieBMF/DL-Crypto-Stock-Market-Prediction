import pandas as pd
from base import Klines
import threading
import signal


def handle_kb_interrupt(sig, frame):
    stop_event.set()

def preparation():
    btc_klines = Klines(
        base_url='https://api.binance.com',
        db_connection_string='postgresql://postgres:postgres@127.0.0.1/CryptoData',
        coin='BTC',
        )
    btc_klines.get_data_from_sql_checkpoints()
    klines = [btc_klines]
    return klines


def loop(klines):
    for i in range(0, 1):
        for coin_type_klines in klines:

            coin_type_klines.update_server_time()

            if not coin_type_klines.continue_actual_period():
                print('Actual period not continued')
                coin_type_klines.change_period()
                continue

            coin_type_klines.get_data_from_sql_cache()

            if coin_type_klines.check_necessary_to_create_a_table():
                coin_type_klines.create_table_for_period()
                coin_type_klines.update_kline_settings('tables')

            coin_type_klines.get_and_save_new_klines()

            coin_type_klines.update_checkpoints_table_in_sql()

            if stop_event.is_set():
                break


if __name__ == '__main__':
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, handle_kb_interrupt)
    klines = preparation()
    thread = threading.Thread(target=loop, args=(klines,))
    thread.start()
    thread.join()
    print('Program done')
