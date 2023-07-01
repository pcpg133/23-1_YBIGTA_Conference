import logging
import sys
from typing import List, Optional, Tuple
import pandas as pd
from datetime import datetime
from dataclasses import dataclass
import plotly.graph_objects as go
import plotly.subplots as sp
from plotly.figure_factory import create_quiver

from binance import Client  
import pandas as pd
from datetime import datetime


client = Client()
data = client.get_historical_klines(
    "ETHUSDT",
    "1h",
    start_str=str(datetime(2022, 1, 1)),
    end_str=str(datetime(2023, 6, 23)),
)
df = pd.DataFrame(data)
# set indexes.
df = df.iloc[:, 0:6]
df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
df.to_csv("data.csv", index=False)

logger = logging.getLogger("backtest")
logger.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(logging.Formatter("%(levelname)s : %(message)s"))
logger.addHandler(stdout_handler)


def load_data():
    df = pd.read_csv("data.csv")
    df["Date"] = pd.to_datetime(df["Date"], unit="ms")
    df.set_index("Date", inplace=True)
    return df

# PREPROCESS

def calc_rsi(series: pd.Series, period: int):
    delta = series.diff()
    gain = delta.mask(delta < 0, 0)
    loss = -delta.mask(delta > 0, 0)
    
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi  


# DATA CLASSES
@dataclass
class PositionAlter:
    date: datetime
    price: float
    is_buy: bool

@dataclass
class PositionVector:
    open_position_event: PositionAlter
    close_position_event: Optional[PositionAlter]


def do_plot(dataframe: pd.DataFrame, title: str, position_vectors: List[PositionVector], position_alters: List[PositionAlter]):
    fig = sp.make_subplots(rows=3, cols=1, shared_xaxes=True, row_heights=[0.2, 0.4, 0.2])

    # Add Equity Trace
    fig.add_trace(
        go.Scatter(x=dataframe.index, y=dataframe['Equity'], mode='lines', name='Equity'),
        row=1, col=1
    )

    # plot all position vectors
    closed_position_vectors = [position_vector for position_vector in position_vectors if position_vector.close_position_event is not None]
    vec_pairs: List[Tuple[Tuple[float, float]]] = [
        (
            (position_vector.open_position_event.date, position_vector.open_position_event.price),
            (position_vector.close_position_event.date, position_vector.close_position_event.price)
        ) for position_vector in closed_position_vectors
    ]

    for ((x1,y1),(x2,y2)) in vec_pairs:
        # did go up?
        if y2 > y1:
            did_go_up = True
        else:
            did_go_up = False
        fig.add_trace(
            go.Scatter(
                x=[x1, x2],
                y=[y1, y2],
                mode='lines',
                line=dict(color= 'green' if did_go_up else 'red', width=5),
                showlegend=False,
                hovertemplate=f"{'Buy' if did_go_up else 'Sell'}<br>Price: {y1}<br>Date: {x1}<extra></extra>"
            ),
            row=2, col=1,
        )


    # Add the price trace to the first subplot
    fig.add_trace(
        go.Scatter(x=df.index, y=df['Close'], mode='lines', name='Close Price'),
        row=2, col=1,
    )

    # Add the RSI trace to the second subplot
    fig.add_trace(
        go.Scatter(x=df.index, y=df['RSI'], mode='lines', name='RSI'),
        row=3, col=1
    )
    
        
    # dot plot for position alters
    # plot blue dot for buy, red dot for sell
    buys = [position_alter for position_alter in position_alters if position_alter.is_buy]
    sells = [position_alter for position_alter in position_alters if not position_alter.is_buy]

    fig.add_trace(
        go.Scatter(
            x=[position_alter.date for position_alter in buys],
            y=[position_alter.price for position_alter in buys],
            mode='markers',
            marker=dict(color='blue', size=5),
            name='Buy'
        ),
        row=2, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=[position_alter.date for position_alter in sells],
            y=[position_alter.price for position_alter in sells],
            mode='markers',
            marker=dict(color='red', size=5),
            name='Sell',
        ),
        row=2, col=1
    )

    # Update layout and axis labels
    fig.update_layout(title='Price and RSI')
    fig.update_xaxes(title_text='Time', row=2, col=1)
    fig.update_yaxes(title_text='Equity', row=1, col=1)
    fig.update_yaxes(title_text='Price', row=2, col=1)
    fig.update_yaxes(title_text='RSI', row=3, col=1)

    fig.show()



# HYPERPARAMETERS
COMISSION = 0.0004
RSI_PERIOD=14



# CLASSES
class Agent:
    def __init__(self, name: str, init_cash: float):
        self.__name = name
        self.__initial_cash = init_cash
        self.__cash: float = init_cash
        self.__asset: float = 0
        self.__opened_price: Optional[float] = None
        self.__position_vector: Optional[PositionVector] = None
        self.__position_vectors = []
        self.__position_alters = []

    def open(self, price: float, date: datetime, is_buy=True):
        if is_buy:
            self.__asset = self.__cash * (1 - COMISSION) / price
            self.__cash = 0
            self.__opened_price = price

            position_alter = PositionAlter(date, price, True)
            self.__position_alters.append(position_alter)
            self.__position_vector = PositionVector(
                position_alter, None
            )
            self.__position_vectors.append(self.__position_vector)
        else:
            #숏 포지션 -> 코인을 거래소에서 빌려오는거기 때문에 asset, 즉 빌려온 코인의 양을 -로 표시함
            self.__asset = -self.__cash * (1 - COMISSION) / price
            #물건을 빌려서 미리 파는게 숏이므로 현금은 현재 상태의 2배가 됨 -> 애초에 빌려올 수 있는 양은 현재 내가 갖고 있는 현금만큼임 
            self.__cash = self.__cash * (1 - COMISSION) * 2
            self.__opened_price = price

            position_alter = PositionAlter(date, price, False)
            self.__position_alters.append(position_alter)
            self.__position_vector = PositionVector(
                position_alter, None
            )
            self.__position_vectors.append(self.__position_vector)

    def get_opened_price(self) -> Optional[float]:
        return self.__opened_price

    def close(self, price: float, date: datetime):
        if self.is_open():
            if self.__asset > 0:
                # Long position close
                self.__cash += abs(self.__asset) * (1 - COMISSION) * price
                self.__asset = 0
                self.__opened_price = None

                position_alter = PositionAlter(date, price, False)
                self.__position_alters.append(position_alter)

                assert self.__position_vector is not None
                self.__position_vector.close_position_event = position_alter

            elif self.__asset < 0:
                
                #빌린 이더를 거래소에 다시 갚는데 드는 비용 
                payBack_price = abs(self.__asset) * price * (1+COMISSION)

                # Update cash and reset asset and opened price values to default values.
                self.__cash -= payBack_price
                self.__asset = 0
                self.__opened_price = None

                position_alter = PositionAlter(date, price, True)
                self.__position_alters.append(position_alter)

                assert self.__position_vector is not None
                self.__position_vector.close_position_event = position_alter

    def is_open(self):
        if abs(self.__asset) > 0:
            assert self.__opened_price is not None
            assert self.__position_vector is not None

        return abs(self.__asset) > 0

    def get_name(self):
        return self.__name

    def get_cash(self):
        return self.__cash

    def get_initial_cash(self):
        return self.__initial_cash

    def get_asset(self):
        return self.__asset

    def get_equity(self, price: float):
        if not self.is_open():
            return self.__cash
        
        elif self.is_open() and self.__asset > 0:
            return self.__cash + self.__asset * price
        
        elif self.is_open() and self.__asset < 0:
            return self.__cash/2 + (self.__opened_price - price) * abs(self.__asset)

    def get_position_vectors(self):
        return self.__position_vectors

    def get_position_alters(self):
        return self.__position_alters

# BACKTESTING CODE
df = load_data()
df["RSI"] = calc_rsi(df["Close"], RSI_PERIOD)

STOP_LOSS_LONG_POSITIONS = 0.015
TAKE_PROFIT_LONG_POSITIONS = 0.03
STOP_LOSS_SHORT_POSITIONS = 0.015
TAKE_PROFIT_SHORT_POSITIONS = 0.03
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 25

agent = Agent("test", 10000)
equity_arr = []
cash_arr = []

#strategy 
short_win = 0 
short_lose = 0 
long_win = 0
long_lose = 0
for i in range(len(df)):
    date = df.index[i]

    current_candle_close = df['Close'][i]
    current_candle_open = df['Open'][i]
    prev_candle_close = df['Close'][i-1] if i > 0 else None
    prev_candle_open = df['Open'][i-1] if i > 0 else None

    is_bullish_engulfing_candle = i > 0 and current_candle_close > current_candle_open and prev_candle_close < prev_candle_open and prev_candle_close >= current_candle_open and prev_candle_open <= current_candle_close if i > 0 else False
    is_bearish_engulfing_candle = i> 0 and current_candle_close < current_candle_open and prev_candle_close > prev_candle_open and prev_candle_close <= current_candle_open and prev_candle_open >= current_candle_close if i > 0 else False

    is_overbought = df['RSI'][i] > RSI_OVERBOUGHT
    is_oversold = df['RSI'][i] < RSI_OVERSOLD

    if agent.is_open():
        if agent.get_asset() > 0:
            # Long position opened
            is_stop_loss_long_position = df['Close'][i] <= (1 - STOP_LOSS_LONG_POSITIONS) * agent.get_opened_price()
            is_take_profit_long_position = df['Close'][i] >= (1 + TAKE_PROFIT_LONG_POSITIONS) * agent.get_opened_price()

            if is_stop_loss_long_position: 
                agent.close(df['Close'][i], date)
                long_lose += 1
            if is_take_profit_long_position:
                agent.close(df['Close'][i], date)
                long_win += 1


        elif agent.get_asset() < 0:
            # Short position opened
            is_stop_loss_short_position = df['Close'][i] >= (1 + STOP_LOSS_SHORT_POSITIONS) * agent.get_opened_price()
            is_take_profit_short_position = df['Close'][i] <= (1 - TAKE_PROFIT_SHORT_POSITIONS) * agent.get_opened_price()

            if is_stop_loss_short_position:
                agent.close(df['Close'][i], date)
                short_lose += 1
            if is_take_profit_short_position:
                agent.close(df['Close'][i], date)
                short_win += 1

    else:
        if is_bullish_engulfing_candle and is_oversold:
            agent.open(df['Close'][i], date, True)
        elif is_bearish_engulfing_candle and is_overbought:
            agent.open(df['Close'][i], date, False)

    agent_equity = agent.get_equity(df['Close'][i])
    agent_cash = agent.get_cash()
    equity_arr.append(agent_equity)
    cash_arr.append(agent_cash)

# PLOT
df['Equity'] = equity_arr
df['Cash'] = cash_arr
do_plot(df, "test", agent.get_position_vectors(), agent.get_position_alters())

# print report
print(f"""
    Initial Cash: {agent.get_initial_cash()}
    Final Equity: {agent.get_equity(df['Close'][len(df)-1])}
    Profit (%): {(((agent.get_equity(df['Close'][len(df)-1])-1) / agent.get_initial_cash())-1)* 100}
    Tries: {len([position_vector for position_vector in agent.get_position_vectors()])}
    Buys: {len([position_alter for position_alter in agent.get_position_alters() if position_alter.is_buy])}
    Sells: {len([position_alter for position_alter in agent.get_position_alters() if not position_alter.is_buy])}
    Total Trade Count: {len(agent.get_position_alters())}
    Win Rate: {(long_win+short_win)*100}/{long_win+short_win+long_lose+short_lose}
    Short Win Rate : {(short_win)}/{short_win+short_lose}
    long Win Rate : {(long_win)}/{long_win+long_lose}
    RSI Oversold/Overbought: {RSI_OVERSOLD}/{RSI_OVERBOUGHT}
    Comission: {COMISSION}
    RSI Period: {RSI_PERIOD}
""")