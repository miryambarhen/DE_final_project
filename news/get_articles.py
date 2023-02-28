import requests
import json
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
articles_col = db["articles"]

# Get ticker names
ticker_names = tickers_col.distinct("ticker")

# Get articles
for ticker in ticker_names:
    # Make API request and get the results as a JSON object
    ticker = ticker
    date = datetime.today().strftime('%Y-%m-%d')
    url = f'https://api.polygon.io/v2/reference/news?ticker={ticker}' \
          f'&published_utc={date}' \
          f'&order=asc&sort=published_utc&apiKey={polygon_key}'
    response = requests.get(url)
    data = response.json()

    # Create a list to hold the dictionaries for the three articles
    articles = []
    # loop through the articles in the API response and extract the data for the first three
    if len(data['results']) > 0:
        for i, article in enumerate(data['results'][:3]):
            article_data = {
                '_id': ticker + ':' + date + ':' + str(i + 1),
                'date': date,
                'ticker': ticker,
                'published_at': article['published_utc'],
                'title': article['title'],
                'publisher': article['publisher']['name'],
                'author': article['author'],
                'article': article['article_url']
            }
            articles.append(article_data)
        articles_col.insert_many(articles)
