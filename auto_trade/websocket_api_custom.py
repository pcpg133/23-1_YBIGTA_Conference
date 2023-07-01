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
COMISSION = None
RSI_PERIOD = 14
STOP_LOSS_LONG_POSITIONS = 0.015
TAKE_PROFIT_LONG_POSITIONS = 0.03
STOP_LOSS_SHORT_POSITIONS = 0.015
TAKE_PROFIT_SHORT_POSITIONS = 0.03
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 25
LEVERAGE = 1

# Binance API 키
API_KEY = ''
API_SECRET = ''

# Binance 계좌 접속
client = Client(API_KEY, API_SECRET)
exchange = ccxt.binance(config={
    'apiKey': API_KEY,
    'secret': API_SECRET,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'       # 선물 거래
    }
})

# 레버리지 설정 및 격리/교차 설정
exchange.set_leverage(LEVERAGE, 'ETH/USDT')
exchange.set_margin_mode(marginMode='isolated', symbol='ETH/USDT')
COMISSION = exchange.market(SYMBOL)['taker'] # 현물거래 지정가/시장가 수수료 = 0.1%, 선물거래 지정가 수수료 = 0.02%, 선물거래 시장가 수수료 = 0.04%


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

    df['Date'] = pd.to_datetime(df['t'], utc=True, unit='ms').dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
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
# DATA CLASSES
# Plot을 그리기 위한 데이터 클래스들
@dataclass
class PositionAlter: # 롱/숏 포지션의 거래 정보를 저장하는 데이터 클래스
    date: datetime
    price: float
    is_buy: bool

@dataclass
class PositionVector: # 롱/숏 포지션 시작과 끝 정보를 저장하는 데이터 클래스
    open_position_event: PositionAlter
    close_position_event: Optional[PositionAlter]

# CLASSES
class Agent:
    def __init__(self, name: str, init_cash: float):
        self.__name = name
        self.__initial_cash = init_cash        # -> 초기 금액(USDT)
        self.__cash: float = init_cash        # -> 현재 가지고 있는 금액(USDT)
        self.__asset: float = 0        # -> 현재 가지고 있는 ETH의 양
        self.__opened_price: Optional[float] = None        # -> 롱/숏포지션 진입 시 가격
        self.__is_position = False        # -> 롱/숏포지션 진입 여부
        self.__is_long = True        # -> 현재 포지션이 롱인지 숏인지 
        self.__position_vector: Optional[PositionVector] = None        # -> open과 close의 거래 포지션 정보를 담은 Positionvector
        self.__position_vectors = []
        self.__position_alters = []
        self.__send_trade_data = {}

    # 롱/숏 포지션 진입
    def open(self, price: float, date: datetime, is_buy=True):
        # 롱 포지션 진입
        if is_buy:
            self.__send_trade_data['@timestamp'] = str(minute_chart['Date'].iloc[-1])
            self.__send_trade_data['Datetime'] = str(minute_chart['Date'].iloc[-1])
            self.__send_trade_data['Position'] = 'long'
            self.__send_trade_data['State'] = 'open'
            self.__send_trade_data['Side'] = 'buy'
            self.__send_trade_data['Price'] = price
            self.__is_position = True
            self.__is_long = True
            self.__opened_price = price

            # 전량 거래할 ETH 규모 설정
            new_balances = exchange.fetch_balance(params={"type" : "future"})
            taker_fee_rate = COMISSION
            usdt_balance = new_balances['USDT']['total']
            eth_price = exchange.fetch_ticker('ETH/USDT')['last']
            available_amount = (usdt_balance / eth_price) / (1 + taker_fee_rate)
            result = exchange.create_market_buy_order(symbol=SYMBOL, amount=available_amount)
            new_balances = exchange.fetch_balance(params={"type" : "future"})
            self.__send_trade_data['Amount'] = available_amount
            self.__send_trade_data['Total'] = price * available_amount
            self.__send_trade_data['PnL'] = -1000
            send_message(json.dumps(self.__send_trade_data), 'custom-transaction.json')

            # 남은 cash와 asset 계산
            positions = new_balances['info']['positions']
            for position in positions:
                if position['symbol'] == SYMBOL:
                    self.__asset = position['notional']
                    self.__cash = float(new_balances['USDT']['total']) - float(position['notional'])
            print(f"long 진입 -> asset: {self.__asset} vs {available_amount}, cash: {self.__cash}, open_price: {self.__opened_price}")

            position_alter = PositionAlter(date, price, True)       # -> 거래 정보를 PositionAlter에 저장
            self.__position_alters.append(position_alter)       
            self.__position_vector = PositionVector(
                position_alter, None
            )
            self.__position_vectors.append(self.__position_vector)
        # 숏 포지션 진입
        else:
            self.__is_position = True
            self.__is_long = False
            self.__opened_price = price
            self.__send_trade_data['@timestamp'] = str(minute_chart['Date'].iloc[-1])
            self.__send_trade_data['Datetime'] = str(minute_chart['Date'].iloc[-1])
            self.__send_trade_data['Position'] = 'short'
            self.__send_trade_data['State'] = 'open'
            self.__send_trade_data['Side'] = 'sell'
            self.__send_trade_data['Price'] = price

            # 전량 거래할 ETH 규모 설정
            new_balances = exchange.fetch_balance(params={"type" : "future"})
            taker_fee_rate = COMISSION
            usdt_balance = new_balances['USDT']['total']
            eth_price = exchange.fetch_ticker('ETH/USDT')['last']
            available_amount = (usdt_balance / eth_price) / (1 + taker_fee_rate)
            result = exchange.create_market_sell_order(symbol=SYMBOL, amount=available_amount)
            new_balances = exchange.fetch_balance(params={"type" : "future"})
            self.__send_trade_data['Amount'] = available_amount
            self.__send_trade_data['Total'] = price * available_amount
            self.__send_trade_data['PnL'] = -1000
            send_message(json.dumps(self.__send_trade_data), 'custom-transaction.json')

            # 남은 cash와 asset 계산
            positions = new_balances['info']['positions']
            for position in positions:
                if position['symbol'] == SYMBOL:
                    self.__asset = position['notional']
                    self.__cash = float(new_balances['USDT']['total']) - float(position['notional'])
            print(f"short 진입 -> asset: {self.__asset} vs {available_amount}, cash: {self.__cash}, open_price: {self.__opened_price}")

            position_alter = PositionAlter(date, price, False)
            self.__position_alters.append(position_alter)
            self.__position_vector = PositionVector(
                position_alter, None
            )
            self.__position_vectors.append(self.__position_vector)

    def get_opened_price(self) -> Optional[float]:
        return self.__opened_price

    # 롱/숏 포지션 청산
    def close(self, price: float, date: datetime):
        if self.is_open():
            # 롱 포지션 청산
            if self.__is_long:
                # Long position close
                self.__is_position = False
                self.__opened_price = self.__send_trade_data['Price']
                self.__send_trade_data['@timestamp'] = str(minute_chart['Date'].iloc[-1])
                self.__send_trade_data['Datetime'] = str(minute_chart['Date'].iloc[-1])
                self.__send_trade_data['Position'] = 'long'
                self.__send_trade_data['State'] = 'close'
                self.__send_trade_data['Side'] = 'sell'
                self.__send_trade_data['Price'] = price
                
                # 전량 거래할 ETH 규모 설정
                new_balances = exchange.fetch_balance(params={"type" : "future"})
                taker_fee_rate = COMISSION
                usdt_balance = new_balances['USDT']['total']
                eth_price = exchange.fetch_ticker('ETH/USDT')['last']
                available_amount = (usdt_balance / eth_price) / (1 + taker_fee_rate)
                result = exchange.create_market_sell_order(symbol=SYMBOL, amount=available_amount)
                new_balances = exchange.fetch_balance(params={"type" : "future"})
                self.__send_trade_data['Amount'] = available_amount
                self.__send_trade_data['Total'] = price * available_amount
                self.__send_trade_data['PnL'] = np.round((price - self.__opened_price) / self.__opened_price * 100, 2)
                send_message(json.dumps(self.__send_trade_data), 'custom-transaction.json')
                
                # 남은 cash와 asset 계산
                self.__asset = 0
                self.__cash = float(new_balances['USDT']['total'])
                self.__opened_price = None
                print(f"long 청산 -> asset: {self.__asset} vs {available_amount}, cash: {self.__cash}, open_price: {self.__opened_price}")

                position_alter = PositionAlter(date, price, False)
                self.__position_alters.append(position_alter)
                assert self.__position_vector is not None
                self.__position_vector.close_position_event = position_alter
            # 숏 포지션 청산
            else:      
                #빌린 이더를 거래소에 다시 갚는데 드는 비용 
                self.__is_position = False
                self.__opened_price = self.__send_trade_data['Price']
                self.__send_trade_data['@timestamp'] = str(minute_chart['Date'].iloc[-1])
                self.__send_trade_data['Datetime'] = str(minute_chart['Date'].iloc[-1])
                self.__send_trade_data['Position'] = 'short'
                self.__send_trade_data['State'] = 'close'
                self.__send_trade_data['Side'] = 'buy'
                self.__send_trade_data['Price'] = price
                
                # 전량 거래할 ETH 규모 설정
                new_balances = exchange.fetch_balance(params={"type" : "future"})
                taker_fee_rate = COMISSION
                usdt_balance = new_balances['USDT']['total']
                eth_price = exchange.fetch_ticker('ETH/USDT')['last']
                available_amount = (usdt_balance / eth_price) / (1 + taker_fee_rate)
                result = exchange.create_market_buy_order(symbol=SYMBOL, amount=available_amount)
                new_balances = exchange.fetch_balance(params={"type" : "future"})
                self.__send_trade_data['Amount'] = available_amount
                self.__send_trade_data['Total'] = price * available_amount
                self.__send_trade_data['PnL'] = np.round((self.__opened_price - price) / price * 100, 2)
                send_message(json.dumps(self.__send_trade_data), 'custom-transaction.json')

                self.__asset = 0
                self.__cash = float(new_balances['USDT']['total'])
                self.__opened_price = None
                print(f"short 청산 -> asset: {self.__asset} vs {available_amount}, cash: {self.__cash}, open_price: {self.__opened_price}")

                position_alter = PositionAlter(date, price, True)
                self.__position_alters.append(position_alter)
                assert self.__position_vector is not None
                self.__position_vector.close_position_event = position_alter

    def is_open(self):
        if self.__is_position:
            assert self.__opened_price is not None
            assert self.__position_vector is not None

        return self.__is_position

    def get_name(self):
        return self.__name

    def get_cash(self):
        return self.__cash

    def get_initial_cash(self):
        return self.__initial_cash

    def get_asset(self):
        return self.__asset

    def get_is_long(self):
        return self.__is_long

    def get_equity(self):
        new_balances = exchange.fetch_balance(params={"type" : "future"})
        if self.is_open():
            assets = new_balances['info']['assets']

            # USDT 잔고 계산
            usdt_balance = 0
            for asset in assets:
                if asset['asset'] == 'USDT':
                    usdt_balance = float(asset['walletBalance'])
            return usdt_balance

        else:
            return float(new_balances['USDT']['total'])

    def get_position_vectors(self):
        return self.__position_vectors

    def get_position_alters(self):
        return self.__position_alters

balances = exchange.fetch_balance(params={"type" : "future"})
cash = float(balances['USDT']['total'])
print(cash)
agent = Agent('test', cash)
equity_arr = []
cash_arr = []

#############################################################################
#############################################################################
#############################################################################
# 메인이 되는 함수
# WebSocket 데이터 수신 콜백 함수
short_win = 0 
short_lose = 0 
long_win = 0
long_lose = 0
INITAIL = True
def handle_socket_message(msg):
    is_candle_closed = msg['k']['x']
 
    if is_candle_closed:
        data = raw_to_filtered_data(msg)
        global minute_chart
        minute_chart = pd.concat([minute_chart, data], axis = 0, ignore_index=True)
        minute_chart['RSI'] = get_rsi(minute_chart['Close'], RSI_PERIOD)
        minute_chart['MACD'], minute_chart['MACD_Signal'] = get_macd(minute_chart)
        minute_chart['CCI'] = get_cci(minute_chart)
        minute_chart['IsBull'] = get_bull_bear(minute_chart)
        minute_chart['IsBullishEngulfing'] = get_bull_bear_engulfing(minute_chart)
        minute_chart['RoR'] = 0.0
        minute_chart = minute_chart[-50:]

        date = minute_chart['Date'].iloc[-1]
        
        current_candle_close = minute_chart['Close'].iloc[-1]
        current_candle_open = minute_chart['Open'].iloc[-1]
        prev_candle_close = minute_chart['Close'].iloc[-2]
        prev_candle_open = minute_chart['Open'].iloc[-2]

        is_bullish_engulfing_candle = current_candle_close > current_candle_open and prev_candle_close < prev_candle_open and prev_candle_close >= current_candle_open and prev_candle_open <= current_candle_close 
        is_bearish_engulfing_candle = current_candle_close < current_candle_open and prev_candle_close > prev_candle_open and prev_candle_close <= current_candle_open and prev_candle_open >= current_candle_close

        is_overbought = minute_chart['RSI'].iloc[-1] > RSI_OVERBOUGHT
        is_oversold = minute_chart['RSI'].iloc[-1] < RSI_OVERSOLD

        # 롱/숏 포지션 청산
        if agent.is_open():
            if agent.get_is_long():
                # 롱 포지션에 진입해 있다면, 롱 포지션 청산하기
                is_stop_loss_long_position = minute_chart['Close'].iloc[-1] <= (1 - STOP_LOSS_LONG_POSITIONS) * agent.get_opened_price()
                is_take_profit_long_position = minute_chart['Close'].iloc[-1] >= (1 + TAKE_PROFIT_LONG_POSITIONS) * agent.get_opened_price()

                if is_stop_loss_long_position:
                    agent.close(minute_chart['Close'].iloc[-1], date)
                    global long_lose 
                    long_lose += 1
                if is_take_profit_long_position:
                    agent.close(minute_chart['Close'].iloc[-1], date)
                    global long_win 
                    long_win += 1

            elif not agent.get_is_long():
                # 숏 포지션에 진입해 있다면, 숏 포지션 청산하기
                is_stop_loss_short_position = minute_chart['Close'].iloc[-1] >= (1 + STOP_LOSS_SHORT_POSITIONS) * agent.get_opened_price()
                is_take_profit_short_position = minute_chart['Close'].iloc[-1] <= (1 - TAKE_PROFIT_SHORT_POSITIONS) * agent.get_opened_price()

                if is_stop_loss_short_position:
                    agent.close(minute_chart['Close'].iloc[-1], date)
                    global short_lose
                    short_lose += 1
                if is_take_profit_short_position:
                    agent.close(minute_chart['Close'].iloc[-1], date)
                    global short_win
                    short_win += 1

        # 롱/숏 포지션 진입
        else:
            # 포지션에 진입해있지 않다면, 롱 포지션 진입하기
            if is_bullish_engulfing_candle and is_oversold:
                agent.open(minute_chart['Close'].iloc[-1], date, True)
            # 포지션에 진입해있지 않다면, 숏 포지션 진입하기
            elif is_bearish_engulfing_candle and is_overbought:    
                agent.open(minute_chart['Close'].iloc[-1], date, False)

        minute_chart['RoR'].iloc[-1] = ((agent.get_equity() / agent.get_initial_cash()) - 1) * 100
        agent_equity = agent.get_equity()
        agent_cash = agent.get_cash()
        equity_arr.append(agent_equity)
        cash_arr.append(agent_cash)

        global INITAIL
        if INITAIL:
            INITAIL = False
            dict_list = dataframe_to_dict(minute_chart)
            for dictionary in dict_list:
                message = json.dumps(dictionary)
                send_message(message, 'raw-data-custom.json')
            producer.flush()
        else:
            dict_list = dataframe_to_dict(minute_chart.iloc[-1].to_frame().T)
            message = json.dumps(dict_list[0])
            send_message(message, 'raw-data-custom.json')
            producer.flush()


# ThreadedWebsocketManager 생성
twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
twm.start()

# 지표 계산을 위한 과거 데이터와 최신 1분봉 시가-종가-고가-저가-볼륨을 담을 dataframe 생성
klines = client.futures_klines(symbol=SYMBOL, interval=Client.KLINE_INTERVAL_1HOUR, start_str=str(datetime(2023, 5, 11, 10)))
minute_chart = get_historical_data(klines)[:-1]

# WebSocket 연결 설정 및 데이터 수신 시작
twm.start_kline_socket(callback=handle_socket_message, symbol=SYMBOL, interval='1h')
twm.join()

# WebSocket 데이터 수신 중단 및 연결 종료
# twm.stop()




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