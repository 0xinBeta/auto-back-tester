from db_handlers import get_filtered_backtest_results
from itertools import groupby
from operator import itemgetter
import datetime
import pandas as pd


def main():
    results = get_filtered_backtest_results()
    # Calculate the date 30 days ago
    thirty_days_ago = datetime.datetime.now() - datetime.timedelta(days=30)

    # Filter results that are within the last 30 days
    recent_results = [
        result for result in results if result[10] >= thirty_days_ago]

    # Sort results by symbol, time frame, and datetime in descending order
    sorted_results = sorted(recent_results, key=lambda x: (
        x[1], x[2], x[10]), reverse=True)

    # Group by symbol and time frame and take the first entry from each group
    grouped_results = []
    for key, group in groupby(sorted_results, key=itemgetter(1, 2)):
        grouped_results.append(next(group))

    # Output the filtered results
    return grouped_results


def create_dataframe(results):
    # Define the column names
    columns = ['id', 'symbol', 'timeframe', 'start_date', 'num_trades',
               'return_percentage', 'winrate', 'max_drawdown', 'tp_m', 'sl_m', 'created_at']

    # Create a DataFrame from the results
    df = pd.DataFrame(results, columns=columns)

    return df


def extract_data(df):
    # Select the columns and convert each row to a dictionary
    selected_columns = ['symbol', 'timeframe', 'tp_m', 'sl_m']
    data_list = df[selected_columns].to_dict(orient='records')

    return data_list


if __name__ == "__main__":
    print(extract_data(create_dataframe(main())))

