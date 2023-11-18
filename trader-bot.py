import asyncio
import ccxt.async_support as ccxt
from df_maker import create_df_async
from dotenv import load_dotenv
import os
import logging
import sys
from datetime import datetime, timedelta

from ret_db import get_trade_parameters

# Load environment variables
load_dotenv()
API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
LIMIT = 500
RISK_PERCENTAGE = 0.01
CALLBACK_RATE = 0.1


def get_rounding_values(symbol):
    round_decimal_price_map = {
        "BTCUSDT": 1,
        "ETHUSDT": 2,
        "ADAUST": 4,
        "SANDUSDT": 4,
        "BNBUSDT": 2,
        "MATICUSDT": 4,
        "XRPUSDT": 4,
        "APEUSDT": 3,
        "LTCUSDT": 2,
        "LINKUSDT": 3,
    }

    round_decimal_pos_map = {
        "BTCUSDT": 3,
        "ETHUSDT": 3,
        "ADAUST": 0,
        "SANDUSDT": 0,
        "BNBUSDT": 2,
        "MATICUSDT": 0,
        "XRPUSDT": 0,
        "APEUSDT": 0,
        "LTCUSDT": 3,
        "LINKUSDT": 2,
    }

    round_decimal_price = round_decimal_price_map.get(
        symbol, 2)  # Default to 2 if symbol not found
    round_decimal_pos = round_decimal_pos_map.get(
        symbol, 2)     # Default to 2 if symbol not found

    return round_decimal_price, round_decimal_pos


async def set_max_leverage(exchange, symbol, coin):
    """
    Fetch the maximum leverage and set it for the given symbol.
    """
    available_tiers = await exchange.fetch_leverage_tiers(symbols=[symbol])
    max_lev = int(available_tiers[f'{coin}/USDT:USDT'][0]['maxLeverage'])
    await exchange.set_leverage(leverage=max_lev, symbol=symbol)
    return max_lev


def calculate_order_details(entry_price, atr, symbol, balance, direction, tp_multiplier, sl_multiplier):
    """
    Calculate the position size, stop loss, and take profit based on entry price, ATR, balance, and multipliers.
    """
    round_decimal_price, round_decimal_pos = get_rounding_values(symbol)

    sl_multiplier = sl_multiplier if direction == 'long' else -sl_multiplier
    tp_multiplier = tp_multiplier if direction == 'long' else -tp_multiplier

    sl = entry_price - round((atr * sl_multiplier), round_decimal_price)
    tp = entry_price + round((atr * tp_multiplier), round_decimal_price)
    distance = abs(entry_price - sl)
    position_size = round((balance * RISK_PERCENTAGE) /
                          distance, round_decimal_pos)

    return position_size, sl, tp


async def place_orders(exchange, symbol, position_size, sl, tp, direction):
    """
    Place the market and limit orders based on the given direction.
    """
    side = 'buy' if direction == 'long' else 'sell'
    opposite_side = 'sell' if direction == 'long' else 'buy'

    # Create market order
    await exchange.create_market_order(symbol=symbol, type="market", side=side, amount=position_size)

    # Create take profit order
    await exchange.create_order(symbol=symbol, type="TRAILING_STOP_MARKET", side=opposite_side, amount=position_size,
                                price=tp, params={"reduceOnly": True, "activationPrice": tp, "callbackRate": CALLBACK_RATE})

    await getattr(exchange, f'create_limit_{opposite_side}_order')(symbol=symbol, amount=position_size, price=sl,
                                                                   params={"stopPrice": sl, "reduceOnly": True})


async def trade_logic(exchange, symbol, timeframe, tp_m, sl_m):
    """
    The core trading logic for a symbol.
    """
    await set_max_leverage(exchange, symbol, coin=symbol[:-4])

    while True:
        try:
            df = await create_df_async(exchange=exchange, symbol=symbol, time_frame=timeframe, limit=LIMIT)

            long_signal = df['long'].iloc[-2]
            short_signal = df['short'].iloc[-2]

            open_position = await exchange.fetch_positions(symbols=[symbol])
            open_pos_condition = int(open_position[0]['contracts'])

            if open_pos_condition == 0:
                if long_signal or short_signal:
                    max_lev = await set_max_leverage(exchange, symbol, coin=symbol[:-4])
                    entry_price = df['Open'].iloc[-1]
                    atr = df['ATR'].iloc[-2]
                    balance = await exchange.fetch_balance()['USDT']['free']
                    direction = 'long' if long_signal else 'short'

                    position_size, sl, tp = calculate_order_details(
                        entry_price, atr, symbol, balance, direction, tp_m, sl_m)

                    _, round_decimal_pos = get_rounding_values(symbol)

                    position_size = round(
                        (position_size * entry_price) / max_lev / balance, round_decimal_pos)

                    await place_orders(exchange, symbol, position_size, sl, tp, direction)

            await asyncio.sleep(1)

        except ccxt.NetworkError as e:
            logging.error(f'NetworkError: {str(e)}')
            await asyncio.sleep(60)
        except Exception as e:
            logging.error(f'An unexpected error occurred: {str(e)}')
            sys.exit()


async def main():
    # Initialize the exchange
    exchange = ccxt.binanceusdm({
        "enableRateLimit": True,
        "apiKey": API_KEY,
        "secret": SECRET_KEY,
    })

    # Initialize last update check
    last_update = datetime.min

    while True:
        current_time = datetime.now()
        if current_time - last_update >= timedelta(days=1):
            trade_params = get_trade_parameters()
            if not trade_params:
                logging.info("No trading today, trade parameters are empty.")
                await asyncio.sleep(86400)  # sleep for 1 day
                continue
            last_update = current_time

        # Update your trading logic with trade_params
        tasks = [trade_logic(exchange, param['symbol'], param['timeframe'],
                             param['tp_m'], param['sl_m']) for param in trade_params]
        await asyncio.gather(*tasks)

        # Add a sleep here to prevent continuous loop without delay
        await asyncio.sleep(1)

    await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
