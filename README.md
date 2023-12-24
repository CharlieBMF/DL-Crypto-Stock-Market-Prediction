# DL-Crypto-Stock-Market-Prediction
Collect data from crypto market, train models to predict future market trend, real time market data analysis.

KAZDA TABELA MA NA POCZATKU KOLUMNY [unixTimestamp] [Timestamp]


ORDERBOOK jest zbierany co petle bez przerwy i wpisywany do tabel w sql
ORDERBOOK tabele sa tworzone co miesiac nowa,
ORDERBOOK tabele maja nazwe OB_1_2023_ALL
ORDERBOOK tabela OB_1_2023_AVG_PER_1MINUTE zawiera usrednione wartosci dla jednej minuty dla rekordow w tej minucie z tabeli OB_1_2023_ALL
ORDERBOOK tabela OB_1_2023_AVG_PER_3MINUTE zawiera usrednione wartosci dla jednej minuty dla rekordow w 3 ostatnich minutach z tabeli OB_1_2023_ALL
OB_1_2023_ALL
OB_1_2023_AVG_PER_1MINUTE, 
OB_1_2023_AVG_PER_3MINUTE, 
OB_1_2023_AVG_PER_5MINUTE,
OB_1_2023_AVG_PER_15MINUTE,
OB_1_2023_AVG_PER_30MINUTE,
OB_2023_AVG_PER_1HOUR,
OB_2023_AVG_PER_2HOUR,
OB_2023_AVG_PER_4HOUR,
OB_2023_AVG_PER_6HOUR,
OB_2023_AVG_PER_8HOUR,
OB_2023_AVG_PER_12HOUR,
OB_2023_AVG_PER_1DAY,
OB_2023_AVG_PER_3DAY,
OB_2023_AVG_PER_1WEEK,
OB_2023_AVG_PER_1MONTH,

KLINY sa minimalnie zbierane co minute
KLINY maja nazwy tabeli
KLINY dane sa pobierane dla kazdej tabeli osobno z /api/v3/klines z uwzglednieniem startTime, endTime i odpowiedniego interwalu czasowego zaleznego od tabeli (od 1 min do 1 month)
KLINY sa wzbogacone o kolumny analizy technicznej z biblioteki stockstats
KLINY jako rekordy sa wpisywane do tabeli w zaleznosci od koniecznosci a w czasie oczekiwania (minimalnie 1 minuta) dokonywany jest update na rekordach dopisujac wskazniki analizy technicznej niezbedne do tego

CLINES_1_2023_ALL_PER_1MINUTE, 
CLINES_1_2023_ALL_PER_3MINUTE, 
CLINES_1_2023_ALL_PER_5MINUTE,
CLINES_1_2023_ALL_PER_15MINUTE,
CLINES_1_2023_ALL_PER_30MINUTE,
CLINES_2023_ALL_PER_1HOUR,
CLINES_2023_ALL_PER_2HOUR,
CLINES_2023_ALL_PER_4HOUR,
CLINES_2023_ALL_PER_6HOUR,
CLINES_2023_ALL_PER_8HOUR,
CLINES_2023_ALL_PER_12HOUR,
CLINES_2023_ALL_PER_1DAY,
CLINES_2023_ALL_PER_3DAY,
CLINES_2023_ALL_PER_1WEEK,
CLINES_2023_ALL_PER_1MONTH,
KLINY co miesciac tworza nowa tabele


TICKER sa zbierane przez /api/v3/ticker ktory umozliwia generowanie dowolnego okna czasowego dla statystyk z przeszlosci
TICKER dane sa pobierane dla tabeli do 1 WEEK osobno z /api/v3/ticker z uwzglednieniem odpowiedniego okna czasowego (od 1 min do 7 days max)
TICKER tabela 1 MONTH generowana jest na bazie danych z tabeli 1WEEK
TICKER_1_2023_ALL_PER_1MINUTE, 
TICKER_1_2023_ALL_PER_3MINUTE, 
TICKER_1_2023_ALL_PER_5MINUTE,
TICKER_1_2023_ALL_PER_15MINUTE,
TICKER_1_2023_ALL_PER_30MINUTE,
TICKER_2023_ALL_PER_1HOUR,
TICKER_2023_ALL_PER_2HOUR,
TICKER_2023_ALL_PER_4HOUR,
TICKER_2023_ALL_PER_6HOUR,
TICKER_2023_ALL_PER_8HOUR,
TICKER_2023_ALL_PER_12HOUR,
TICKER_2023_ALL_PER_1DAY,
TICKER_2023_ALL_PER_3DAY,
TICKER_2023_ALL_PER_1WEEK,
TICKER_2023_ALL_PER_1MONTH,


ZBIORCZA TABELA TO SUMA KOLUMN ZE WSZYSTKICH TABEL
ZBIORCZA TABELA ma nazwe 
FINAL_1_2023_PER_1MINUTE
FINAL_1_2023_ALL_PER_1MINUTE, 
FINAL_1_2023_ALL_PER_3MINUTE, 
FINAL_1_2023_ALL_PER_5MINUTE,
FINAL_1_2023_ALL_PER_15MINUTE,
FINAL_1_2023_ALL_PER_30MINUTE,
FINAL_2023_ALL_PER_1HOUR,
FINAL_2023_ALL_PER_2HOUR,
FINAL_2023_ALL_PER_4HOUR,
FINAL_2023_ALL_PER_6HOUR,
FINAL_2023_ALL_PER_8HOUR,
FINAL_2023_ALL_PER_12HOUR,
FINAL_2023_ALL_PER_1DAY,
FINAL_2023_ALL_PER_3DAY,
FINAL_2023_ALL_PER_1WEEK,
FINAL_2023_ALL_PER_1MONTH,

WSZYSTIKIE ZBIORCZE TABELE maja dodatkowo kolumny
CURRENT AVARAGE PRICE


DOCKER A zajmuje sie zbieraniem danych dla minimalnych tabel tzn dla OB_1_2023_ALL 	CLINES_1_2023_ALL_PER_1MINUTE,
