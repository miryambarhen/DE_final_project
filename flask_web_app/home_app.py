import json
import os
import mysql.connector
from pymongo import MongoClient
from flask import Flask, render_template, request, jsonify

# Get configuration data
with open('/tmp/pycharm_project_4/config.json') as f:
    config = json.load(f)

# Connect to MongoDB database
client = MongoClient('mongodb://localhost:27017/')
db = client['stocks_db']
col = db["realtime_data"]
users = db["users"]

# Connect to MySQL database
mysql_conn = mysql.connector.connect(
    host="localhost",
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database="stocks"
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or "my-secret-key"


@app.route('/')
def index():
    try:
        # retrieve stock data from MongoDB
        stock_data = col.aggregate([
            {"$sort": {"stock_ticker": 1, "time": -1}},
            {
                "$group": {
                    "_id": "$stock_ticker",
                    "current_price": {"$first": "$current_price"},
                    "last_time": {"$first": "$time"}
                }
            }
        ])
        stock_data = list(stock_data)
        # add price change based on daily_stocks table
        cursor = mysql_conn.cursor()
        for stock in stock_data:
            ticker = stock['_id']
            # find the maximum date in daily_stocks table
            cursor.execute(f"SELECT MAX(business_day) FROM daily_stocks WHERE ticker='{ticker}'")
            max_date = cursor.fetchone()[0]
            if max_date:
                # get the close price for the maximum date
                cursor.execute(
                    f"SELECT close_price FROM daily_stocks WHERE ticker='{ticker}' AND business_day='{max_date}'")
                close_price = cursor.fetchone()[0]
                # calculate price change
                price_change = round(((stock['current_price'] - close_price) / close_price) * 100, 2)
                stock['price_change'] = price_change
            else:
                stock['price_change'] = None
        # Sort the stock data by ticker symbol in ascending order
        stock_data = sorted(stock_data, key=lambda x: x['_id'])
        # pass stock data to template
        return render_template('home.html', prices=stock_data)

    except Exception as e:
        # log error
        app.logger.error(str(e))
        # return error message to user
        return "Error occurred while retrieving stock data.", 500


@app.route('/register', methods=["GET", "POST"])
def register():
    data = request.form.to_dict()
    if len(data) > 0:
        # Add an indicator for active alert
        data.update({'is_active': 1})
        # Save registration form in MonogoDB
        users.insert_one(data)
    return render_template("registration.html")


if __name__ == '__main__':
    app.run(debug=True)
