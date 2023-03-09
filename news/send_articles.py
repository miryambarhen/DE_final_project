import utilities
from datetime import datetime
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["stocks_db"]
users_col = db["users"]
articles_col = db["articles"]

# Get users choices data
alerts = users_col.find({'news': 'on', 'is_active': 1},
                        {'first_name': 1, 'email_address': 1, 'stock_ticker': 1, '_id': 0})

# Send email
for user in alerts:
    recipient = user['email_address']
    ticker = user['stock_ticker']
    name = user['first_name'][0].upper() + user['first_name'][1:]
    # Get articles from articles_col
    date = datetime.today().strftime('%Y-%m-%d')
    articles = articles_col.find({'date': date, 'ticker': ticker},
                                 {'title': 1, 'publisher': 1, 'article': 1, '_id': 0})
    if articles.count() > 0:
        subject = f'Articles about {ticker} stock published today'
        # Create the body of the email
        body = f'Hi {name},\n\n\nBelow are articles that may interest you:\n\n'
        for article in articles:
            body += f'{article["title"]} ({article["publisher"]}) -\n {article["article"]} \n\n'
        body += '\n\nbest regards,\nNaya Trades Team'
        # Message for log
        message = f'Articles about {ticker} were sent to {recipient}'
        # Call to send_email function
        utilities.send_email(recipient, subject, body, message)
