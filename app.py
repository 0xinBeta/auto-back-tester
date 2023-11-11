import multiprocessing as mp
import vectorbt as vbt
from backtesting import Backtest
from backtesting.lib import Strategy
from finta import TA
from ta.trend import adx
import time
import ccxt
import sys
from db_handlers import insert_backtest_result

DEFAULT_CASH = 100_000
DEFAULT_MARGIN = 0.008
DEFAULT_SYMBOL_OPTIONS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SANDUSDT",
                          "MATICUSDT", "APEUSDT", "LTCUSDT", "LINKUSDT",
                          "XRPUSDT", "ADAUSDT"]
DEFAULT_TIMEFRAME_OPTIONS = ["5m", "15m", "1h", "1d"]
DEFAULT_START_OPTIONS = ["30 day ago UTC", "90 day ago UTC", "180 day ago UTC"]
DEFAULT_END = "now"
EXCHANGE_NAME = "binanceusdm"

# Other configurations
EMA_SHORT_LEN = 9
EMA_MID_LEN = 21
EMA_LONG_LEN = 50
ADX_INDEX = 20

def create_df(symbol: str, timeframe: str, start: str, end: str,
              ema_short_len: int, ema_mid_len: int, ema_long_len: int,
              adx_index: int):
    """Download and compute technical indicators for a given symbol and time frame."""
    df = vbt.CCXTData.download(
        symbols=symbol,
        missing_index="drop",
        exchange=EXCHANGE_NAME,
        timeframe=timeframe,
        start=start,
        end=end
    ).get()

    # Calculate EMAs and SMMA
    for length, column in [(ema_short_len, 'ema_short'), (ema_mid_len, 'ema_mid'), (ema_long_len, 'ema_long')]:
        df[column] = TA.EMA(df, length, 'close')
    df['SMMA200'] = TA.SMMA(df, period=200)

    # Identify crossovers between short and mid EMAs
    df['cross_above'] = df['ema_short'].gt(
        df['ema_mid']) & df['ema_short'].shift(1).lt(df['ema_mid'].shift(1))
    df['cross_below'] = df['ema_short'].lt(
        df['ema_mid']) & df['ema_short'].shift(1).gt(df['ema_mid'].shift(1))

    # Calculate additional technical indicators
    df['ADX'] = adx(df['High'], df['Low'], df['Close'], window=14)
    df['ATR'] = TA.ATR(df, 14)
    df['RSI'] = TA.RSI(df, period=14)

    # Determine long and short trading opportunities
    df['long'] = (
        (df['Close'] > df['SMMA200']) &
        (df['ema_short'] > df['SMMA200']) &
        (df['ema_mid'] > df['SMMA200']) &
        (df['RSI'] < 75) &
        (df['ema_mid'] > df['ema_long']) &
        df['cross_above'] &
        (df['ADX'] > adx_index) &
        (df['ema_long'] > df['SMMA200'])
    )
    df['short'] = (
        (df['Close'] < df['SMMA200']) &
        (df['ema_short'] < df['SMMA200']) &
        (df['ema_mid'] < df['SMMA200']) &
        (df['RSI'] > 20) &
        (df['ema_mid'] < df['ema_long']) &
        df['cross_below'] &
        (df['ADX'] > adx_index) &
        (df['ema_long'] < df['SMMA200'])
    )

    return df


class QuantitativeModelStrategy(Strategy):
    """Strategy that defines buy and sell signals based on technical indicators."""
    tp_m = 7
    sl_m = 4

    def init(self):
        pass

    def next(self):
        buy_signal = self.data.long[-1]
        sell_signal = self.data.short[-1]
        price = self.data.Close[-1]
        atr = self.data.ATR[-1]
        tp_long = price + atr * self.tp_m
        sl_long = price - atr * self.sl_m
        tp_short = price - atr * self.tp_m
        sl_short = price + atr * self.sl_m

        distance_long = price - sl_long
        distance_short = sl_short - price

        cash = 100_000

        risk_long = (cash * 0.02) / distance_long
        risk_short = (cash * 0.02) / distance_short

        pos_size_long = ((risk_long * price) / 125) / cash
        pos_size_short = ((risk_short * price) / 125) / cash

        if buy_signal:
            if not self.position:
                if pos_size_long < 1:
                    self.buy(size=pos_size_long, sl=sl_long, tp=tp_long)
        elif sell_signal:
            if not self.position:
                if pos_size_short < 1:
                    self.sell(size=pos_size_short, sl=sl_short, tp=tp_short)

def backtest_strategy(strategy_params):
    
    symbol, timeframe, start = strategy_params

    df = create_df(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=DEFAULT_END,
        ema_short_len=EMA_SHORT_LEN,
        ema_mid_len=EMA_MID_LEN,
        ema_long_len=EMA_LONG_LEN,
        adx_index=ADX_INDEX
    )

    # Run the backtest
    bt = Backtest(df, QuantitativeModelStrategy,
                  cash=DEFAULT_CASH, margin=DEFAULT_MARGIN)
    stats = bt.optimize(
                tp_m=range(5,11),
                sl_m=range(1,5),
                maximize='Win Rate [%]' and 'Return [%]' and 'Sharpe Ratio' and 'Sortino Ratio',
                method='grid'
            )
    tp_m_value = stats._strategy.tp_m
    sl_m_value = stats._strategy.sl_m

    # Here you can customize the data that you want to collect from each backtest
    result = {
        'symbol': symbol,
        'timeframe': timeframe,
        'start': start,
        '# Trades': stats['# Trades'],
        'return': stats['Return [%]'],
        'winrate': stats['Win Rate [%]'],
        'max_drawdown': stats['Max. Drawdown [%]'],
        'tp_m': tp_m_value,
        'sl_m': sl_m_value
    }

    return result

def run_backtests_sequentially():
    # Generate all combinations of symbols and timeframes
    for symbol in DEFAULT_SYMBOL_OPTIONS:
        for timeframe in DEFAULT_TIMEFRAME_OPTIONS:
            for start in DEFAULT_START_OPTIONS:
                strategy_params_combination = (symbol, timeframe, start)
                result = backtest_strategy(strategy_params_combination)
                # Save or print the results
                insert_backtest_result(result)
                time.sleep(600)

if __name__ == "__main__":
    mp.set_start_method('fork', force=True)
    while True:
        try: 
            run_backtests_sequentially()
        except ccxt.RequestTimeout as e:
            print(type(e).__name__, str(e))
            time.sleep(60)
        except ccxt.DDoSProtection as e:
            # recoverable error, you might want to sleep a bit here and retry later
            print(type(e).__name__, str(e))
            time.sleep(60)
        except ccxt.ExchangeNotAvailable as e:
            # recoverable error, do nothing and retry later
            print(type(e).__name__, str(e))
            time.sleep(60)
        except ccxt.NetworkError as e:
            # do nothing and retry later...
            print(type(e).__name__, str(e))
            time.sleep(60)
        except Exception as e:
            # panic and halt the execution in case of any other error
            print(type(e).__name__, str(e))
            sys.exit()

        
