import smtplib
import json
from email.mime.text import MIMEText
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from pymongo import MongoClient

# Get configuration data
with open('/tmp/pycharm_project_4/config.json') as f:
    config = json.load(f)

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
    name = user['first_name'][0].upper()+user['first_name'][1:]
    # Get articles from articles_col
    date = datetime.today().strftime('%Y-%m-%d')
    articles = articles_col.find({'date': date, 'ticker': ticker},
                                 {'title': 1, 'publisher': 1, 'article': 1, '_id': 0})
    if articles.count()>0:
        msg = MIMEMultipart()
        msg['From'] = 'Naya Trades'
        msg['To'] = recipient
        msg['Subject'] = f'Articles about {ticker} stock published today'
        # Create the body of the email
        body = f'Hi {name},\n\n\nBelow are articles that may interest you:\n\n'
        for article in articles:
            body += f'{article["title"]} ({article["publisher"]}) -\n {article["article"]} \n\n'
        body += '\n\nkind regards,\nNaya Trades team'
        # Attach the body to the email message
        msg.attach(MIMEText(body, 'plain'))
        # Connect to the SMTP server
        server = smtplib.SMTP(config['email']['smtp_server'], config['email']['smtp_port'])
        server.starttls()
        server.login(config['email']['sender'], config['email']['password'])
        # Send the email
        text = msg.as_string()
        server.sendmail(msg['From'], msg['To'], text)
        # Close the connection to the SMTP server
        server.quit()


