import pandas as pd
from finta import TA
from ta.trend import adx

async def create_df_async(exchange, symbol, time_frame, limit):
    """
    Generates a pandas DataFrame with technical indicators and trading opportunities based on a specific set of conditions.

    Args:
        exchange: (ccxt.Exchange) Exchange object
        symbol: (str) Trading pair symbol
        timeframe: (str) Timeframe for candlestick data (e.g. '1h', '4h', '1d')
        limit: (int) Maximum number of candles to retrieve

    Returns:
        df: (pd.DataFrame) DataFrame with calculated technical indicators and identified long/short trading opportunities
    """
    candles = await exchange.fetch_ohlcv(symbol=symbol, timeframe=time_frame,
                                   limit=limit)
    df = pd.DataFrame(candles, columns=[
        'DateTime',
        'Open',
        'High',
        'Low',
        'Close',
        'Volume',
        ])
    
    df.DateTime = pd.to_datetime(df.DateTime, unit='ms')

    df['EMA9'] = TA.EMA(df, 9, 'close')
    df['EMA21'] = TA.EMA(df, 21, 'close')
    df['EMA50'] = TA.EMA(df, 50, 'close')
    df['SMMA200'] = TA.SMMA(df, period=200)

    # Identify crossovers between EMA9 and EMA21

    df['cross_above'] = (df['EMA9'] > df['EMA21']) & (df['EMA9'
            ].shift(1) < df['EMA21'].shift(1))
    df['cross_below'] = (df['EMA9'] < df['EMA21']) & (df['EMA9'
            ].shift(1) > df['EMA21'].shift(1))

    # Calculate additional technical indicators

    df['ADX'] = adx(df['High'], df['Low'], df['Close'])
    df['ATR'] = TA.ATR(df, 14)
    df['RSI'] = TA.RSI(df, period=14)

    # Determine long and short trading opportunities based on set conditions

    df['long'] = (df['Close'] > df['SMMA200']) & (df['EMA9']
            > df['SMMA200']) & (df['EMA21'] > df['SMMA200']) & (df['RSI'
            ] < 75) & (df['EMA21'] > df['EMA50']) & df['cross_above'] \
        & (df['ADX'] > 20) & (df['EMA50'] > df['SMMA200'])
    df['short'] = (df['Close'] < df['SMMA200']) & (df['EMA9']
            < df['SMMA200']) & (df['EMA21'] < df['SMMA200']) & (df['RSI'
            ] > 20) & (df['EMA21'] < df['EMA50']) & df['cross_below'] \
        & (df['ADX'] > 20) & (df['EMA50'] < df['SMMA200'])

    
    return df