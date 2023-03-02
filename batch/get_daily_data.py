import requests
import logs.logging_config as logging_config
import datetime

# configure the logging system
logger = logging_config.setup_logging()


# Get data
def get_data(polygon_key, ticker_names, _from, to):
    data_for_loading = []
    for ticker in ticker_names:
        # Make API request and get the results as a JSON object
        ticker = ticker
        url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}' \
              f'/range/1/' \
              f'day/' \
              f'{_from}/' \
              f'{to}' \
              f'?adjusted=true&sort=asc&apiKey={polygon_key}'
        response = requests.get(url)
        data = response.json()
        if 'results' in data:
            for d in data['results']:
                timestamp = d['t'] / 1000.0  # Divide by 1000 to convert milliseconds to seconds
                datetime_obj = datetime.datetime.fromtimestamp(timestamp)  # Convert the timestamp to a datetime object
                date = datetime_obj.date() # Extract the date from the datetime object
                stocks_data = [ticker, d['v'], d['vw'], d['o'], d['c'], d['h'], d['l'], date, d['n']]
                data_for_loading.append(stocks_data)
            logger.info(f"Get data from the polygon API about {ticker} on {date}")
        else:
            logger.warning(f"No data exists for {ticker} on the {date}")
    return data_for_loading


def load_data(db, cursor, polygon_key, ticker_names, _from, to):
    data_for_loading = get_data(polygon_key, ticker_names, _from, to)
    for data_row in data_for_loading:
        ticker = data_row[0]
        date = data_row[7]
        cursor.execute("SELECT id FROM daily_stocks WHERE ticker = %s AND business_day = %s", (ticker, date))
        result = cursor.fetchone()
        if result:
            logger.info(f"Data for ticker {ticker} on {date} already exists in the table")
        else:
            # S# Insert the new data into the table
            sql = "INSERT INTO daily_stocks (ticker, volume, volume_weighted, open_price, close_price, highest_price, lowest_price, business_day, transactions_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(sql, data_for_loading)
            db.commit()
            # Print the number of rows inserted
            logger.info(f"{cursor.rowcount} rows inserted for {date}")
            db.close()
