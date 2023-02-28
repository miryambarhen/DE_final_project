import requests
import json
import logging
import mysql.connector
from datetime import datetime
from pymongo import MongoClient

# Get configuration data
with open('/tmp/pycharm_project_4/config.json') as f:
    config = json.load(f)
polygon_key = config['polygon_key']

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["stocks_db"]
tickers_col = db["tickers"]

# Connect to MySQL database
db = mysql.connector.connect(
    host="localhost",
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database="stocks"
)

# Create a cursor object
cursor = db.cursor()

# Get data
def get_data(ticker_names, _from, to):
    data_for_loading = []
    for ticker in ticker_names:
        # Make API request and get the results as a JSON object
        ticker = ticker
        date = datetime.today().strftime('%Y-%m-%d')
        url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}' \
              f'/range/1/' \
              f'day/' \
              f'{_from}/' \
              f'{to}' \
              f'?adjusted=true&sort=asc&apiKey={polygon_key}'
        response = requests.get(url)
        data = response.json()
        for d in data['results']:
            timestamp = d['t'] / 1000.0  # Divide by 1000 to convert milliseconds to seconds
            date = datetime.fromtimestamp(timestamp)
            stocks_data = [ticker, d['v'], d['vw'], d['o'], d['c'], d['h'], d['l'], date, d['n']]
            data_for_loading.append(stocks_data)
    return data_for_loading


def load_data(ticker_names, _from, to):
    data_for_loading = get_data(ticker_names, _from, to)
    for data_row in data_for_loading:
        ticker = data_row[0]
        date = data_row[7]
        cursor.execute("SELECT id FROM daily_stocks WHERE ticker = %s AND date = %s", (ticker, date))
        result = cursor.fetchone()
        if result:
            print(f"Data for ticker {ticker} on {date} already exists in the table.")
        else:
            print('aaa')
            # S# Insert the new data into the table
            sql = "INSERT INTO daily_stocks (ticker, volume, volume_weighted, open_price, close_price, highest_price, lowest_price, date, transactions_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(sql, data_for_loading)
            db.commit()
            # Print the number of rows inserted
            print(cursor.rowcount, "rows inserted")
            db.close()

# Get ticker names
ticker_names = tickers_col.distinct("ticker")
ticker_names = ['AAPL','AMZN']
_from = '2023-02-24'
to = '2023-02-27'
load_data(ticker_names, _from, to)