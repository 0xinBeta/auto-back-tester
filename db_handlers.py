import psycopg2
from psycopg2 import sql
from urllib.parse import urlparse
import os
from dotenv import load_dotenv



def connect_to_db():
    load_dotenv()

    # Your Neon database URL
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Parse the database URL
    parsed_url = urlparse(DATABASE_URL)

    # Connect to the database
    conn = psycopg2.connect(
        dbname=parsed_url.path[1:],
        user=parsed_url.username,
        password=parsed_url.password,
        host=parsed_url.hostname,
        port=parsed_url.port
    )

    return conn

def insert_backtest_result(result):
    """Insert a new backtest result into the database."""
    conn = connect_to_db()
    cur = conn.cursor()
    query = sql.SQL("""
        INSERT INTO backtest_results (symbol, timeframe, start_date, num_trades, return_percentage, winrate, max_drawdown, tp_m, sl_m)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    cur.execute(query, (
        result['symbol'], 
        result['timeframe'], 
        result['start'], 
        int(result['# Trades']),  # Convert numpy.int64 to Python int
        round(float(result['return']),2),   # Convert numpy.float64 to Python float
        round(float(result['winrate']),2),  # Convert numpy.float64 to Python float
        round(float(result['max_drawdown']),2), # Convert numpy.float64 to Python float
        int(result['tp_m']),       # Convert numpy.int64 to Python int
        int(result['sl_m'])        # Convert numpy.int64 to Python int
    ))
    conn.commit()
    cur.close()
    conn.close()
