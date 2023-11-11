import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

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

# Create a cursor object
cursor = conn.cursor()

# Create the table
create_table_query = '''
CREATE TABLE backtest_results (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50),
    timeframe VARCHAR(20),
    start_date VARCHAR(50),
    num_trades INT,
    return_percentage FLOAT,
    winrate FLOAT,
    max_drawdown FLOAT,
    tp_m INT,
    sl_m INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''

cursor.execute(create_table_query)

# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
