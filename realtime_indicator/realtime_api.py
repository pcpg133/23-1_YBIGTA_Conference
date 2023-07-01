# pip install python-binance, datatime, requests, ccxt, confluent_kafka 등 설치 후, python websocket_api_test.py로 실행
from binance import ThreadedWebsocketManager
from binance.client import Client
import ccxt
import sys, json, requests, time, math
import numpy as np
import pandas as pd
from datetime import datetime
from typing import List, Optional, Tuple
from dataclasses import dataclass
from confluent_kafka import Producer
import socket

# 각종 설정값 세팅
SYMBOL = 'ETHUSDT'
RSI_PERIOD = 14

# Binance API 키
API_KEY = ''
API_SECRET = ''

# Binance 계좌 접속
client = Client(API_KEY, API_SECRET)

#############################################################################
#############################################################################
#############################################################################
# kafka 및 elastic search로 로그 데이터 전송
conf = {'bootstrap.servers': "",
        'client.id': socket.gethostname()}

producer = Producer(conf)

# kafka의 토픽으로 데이터를 전송하는 함수
def send_message(message, topic: str):
    producer.produce(topic, value=message)  # 메시지를 지정한 토픽으로 보냄
    producer.poll(1)  # Kafka로 메시지를 전송하고 응답을 기다림

# 데이터프레임 행 하나하나를 딕셔너리 형태로 만들어서 리스트로 리턴하는 함수
def row_to_dict(row):
    return {column: str(value) for column, value in row.items()}

def dataframe_to_dict(df):
    dict_list = [row_to_dict(row) for _, row in df.iterrows()]
    return dict_list


#############################################################################
#############################################################################
#############################################################################
# Preprocessing용 함수들

# USDT/KRW 환율 정보를 가져오는 함수
def get_usdt_krw_price():
    url = 'https://api.coingecko.com/api/v3/simple/price'
    params = {
        'ids' : 'tether',
        'vs_currencies' : 'krw'
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    return data['tether']['krw']

# 지표 계산을 위한 과거 데이터를 dataframe으로 불러오는 함수
def get_historical_data(data):
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                     'quote_asset_volume', 'trades', 'taker_buy_base_asset_volume',
                                     'taker_buy_quote_asset_volume', 'ignore'])
    
    df['Date'] = pd.to_datetime(df['timestamp'], utc=True, unit='ms').dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    df['@timestamp'] = df['Date']
    df['Open'] = df['open'].astype(float)
    df['Close'] = df['close'].astype(float) 
    df['High'] = df['high'].astype(float)
    df['Low'] = df['low'].astype(float)
    df['Volume'] = df['volume'].astype(float)

    return df[['@timestamp', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

# Binance API에서 가져온 raw data를 특정 column만 추출한 filtered_data로 변환하는 함수
def raw_to_filtered_data(data):
    df = pd.DataFrame([data['k']], columns=['t', 'T', 's', 'i', 'f', 'L', 'o', 'c', 'h', 'l', 'v', 'n', 'x', 'q', 'V', 'Q', 'B'])

    df['Date'] = pd.DataFrame([data['E']])
    df['Date'] = pd.to_datetime(df['Date'], utc=True, unit='ms').dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['@timestamp'] = df['Date']
    df['Open'] = df['o'].astype(float)
    df['Close'] = df['c'].astype(float)
    df['High'] = df['h'].astype(float)
    df['Low'] = df['l'].astype(float)
    df['Volume'] = df['v'].astype(float)

    return df[['@timestamp', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

# rsi를 구하는 함수
def get_rsi(series: pd.Series, period: int):
    delta = series.diff()
    gain = delta.mask(delta < 0, 0)
    loss = -delta.mask(delta > 0, 0)
    
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

# macd를 구하는 함수
def get_macd(data: pd.DataFrame, n_fast=12, n_slow=26, n_signal=9):
    # 지수이동평균(EMA) 계산
    temp = data.copy()
    ema_fast = temp['Close'].ewm(span=n_fast, min_periods=n_fast - 1).mean()
    ema_slow = temp['Close'].ewm(span=n_slow, min_periods=n_slow - 1).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=n_signal, min_periods=n_signal - 1).mean()
    return macd_line, macd_signal

# cci를 구하는 함수
def get_cci(data: pd.DataFrame, n=20):
    temp = data.copy()
    tp = (temp['High'] + temp['Low'] + temp['Close']) / 3
    ma = tp.rolling(window=n).mean()
    md = tp.rolling(window=n).apply(lambda x: np.fabs(x - x.mean()).mean(), raw=True)
    cci = (tp - ma) / (0.015 * md)
    return cci

# bear/bull을 구하는 함수
def get_bull_bear(data: pd.DataFrame):
    temp = data.copy()
    temp['IsBull'] = (temp['Close'] >= temp['Open']).astype(int)
    return temp['IsBull']

# bear engulfing, bullish engulfing을 구하는 함수
def get_bull_bear_engulfing(data: pd.DataFrame):
    temp = data.copy()
    temp['IsBullishEngulfing'] = -1

    for i in range(1, len(temp)):
        current_candle_close = minute_chart['Close'].iloc[i]
        current_candle_open = minute_chart['Open'].iloc[i]
        prev_candle_close = minute_chart['Close'].iloc[i-1]
        prev_candle_open = minute_chart['Open'].iloc[i-1]

        if current_candle_close > current_candle_open and prev_candle_close < prev_candle_open and prev_candle_close >= current_candle_open and prev_candle_open <= current_candle_close:
            temp.at[i, 'IsBullishEngulfing'] = 1
        elif current_candle_close < current_candle_open and prev_candle_close > prev_candle_open and prev_candle_close <= current_candle_open and prev_candle_open >= current_candle_close:
            temp.at[i, 'IsBullishEngulfing'] = 0
    
    return temp['IsBullishEngulfing']


#############################################################################
#############################################################################
#############################################################################
# 메인이 되는 함수
# WebSocket 데이터 수신 콜백 함수
def handle_socket_message(msg):
    data = raw_to_filtered_data(msg)
    global minute_chart
    minute_chart = pd.concat([minute_chart, data], axis = 0, ignore_index=True)
    minute_chart['RSI'] = get_rsi(minute_chart['Close'], RSI_PERIOD)
    minute_chart['MACD'], minute_chart['MACD_Signal'] = get_macd(minute_chart)
    minute_chart['CCI'] = get_cci(minute_chart)
    minute_chart['IsBull'] = get_bull_bear(minute_chart)
    minute_chart['IsBullishEngulfing'] = get_bull_bear_engulfing(minute_chart)
    minute_chart = minute_chart[-50:]
    dict_list = dataframe_to_dict(minute_chart.iloc[-1].to_frame().T)
    message = json.dumps(dict_list[0])
    send_message(message, 'indicator.json')
    producer.flush()

# ThreadedWebsocketManager 생성
twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
twm.start()

# 지표 계산을 위한 과거 데이터와 최신 1분봉 시가-종가-고가-저가-볼륨을 담을 dataframe 생성
klines = client.futures_klines(symbol=SYMBOL, interval=Client.KLINE_INTERVAL_1MINUTE, start_str=str(datetime(2023, 5, 11, 10)))
minute_chart = get_historical_data(klines)[:-1]

# WebSocket 연결 설정 및 데이터 수신 시작
twm.start_kline_socket(callback=handle_socket_message, symbol=SYMBOL, interval='1m')
twm.join()


# e: 이벤트 타입입니다. Kline 캔들스틱 데이터를 수신하는 경우, 이 값은 'kline'입니다. 
# E: 이벤트 발생 시간입니다. 타임스탬프 형식으로 표시됩니다.
# s: 심볼입니다. 캔들스틱 데이터가 해당 심볼에 대한 것임을 나타냅니다.
# k: 캔들스틱 데이터를 포함하는 객체입니다.
#     t: 캔들스틱의 시작 시간입니다. 타임스탬프 형식으로 표시됩니다.
#     T: 캔들스틱의 종료 시간입니다. 타임스탬프 형식으로 표시됩니다.
#     s: 심볼입니다. 캔들스틱 데이터가 해당 심볼에 대한 것임을 나타냅니다.
#     i: 캔들스틱의 시간 간격입니다.
#     f: 첫 번째 틱(거래)의 타임스탬프입니다.
#     L: 마지막 틱(거래)의 타임스탬프입니다.
#     o: 캔들스틱의 시작 가격입니다.
#     c: 캔들스틱의 종료 가격입니다.
#     h: 캔들스틱의 최고 가격입니다.
#     l: 캔들스틱의 최저 가격입니다.
#     v: 캔들스틱의 거래량입니다.
#     n: 캔들스틱에 포함된 거래의 수입니다.
#     x: 캔들스틱의 완료 여부를 나타내는 플래그입니다. True는 완료된 캔들스틱을 의미하고, False는 아직 완료되지 않은 캔들스틱을 의미합니다.
#     q: 캔들스틱의 종료 시점에서의 누적 거래량입니다.
#     V: 캔들스틱의 종료 시점에서의 누적 거래량입니다. (외부 거래소의 경우)
#     Q: 캔들스틱의 종료 시점에서의 누적 거래량의 가중 평균 가격입니다. (외부 거래소의 경우)