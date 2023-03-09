import json
import utilities
from pymongo import MongoClient
from bson.objectid import ObjectId
from kafka import KafkaConsumer

# for the consumer
bootstrapServers = "cnt7-naya-cdh63:9092"
topic4 = 'users_emails'

# to update is_active
client = MongoClient('mongodb://localhost:27017')
mongo_db = client['stocks_db']
mongo_collection = mongo_db['users']

consumer = KafkaConsumer(topic4, bootstrap_servers=bootstrapServers, auto_offset_reset='latest')

for message in consumer:
    request = json.loads(message.value)
    if request:
        request_id = request['_id']
        stock_ticker = request['stock_ticker']
        name = request['first_name'][0].upper() + request['first_name'][1:]
        wanted_price = float(request['price'])
        recipient = request['email_address']

        # Set alert request to un active
        mongo_collection.update_one({"_id": ObjectId(request_id)}, {"$set": {"is_active": 0}})

        subject = f'{stock_ticker} got to the price you wanted!'
        body = f'Hi {name},\n\n\n' \
               f'Further to your request, we inform you that the {stock_ticker} stock has reached a price of {wanted_price:.2f}$\n\n' \
               f'To submit another request, you are welcome to enter the form again, and we will be happy to track the stocks for you :)'
        body += '\n\nbest regards,\nNaya Trades Team'
        # Message for log
        message = f'Alert about {stock_ticker} was sent to {recipient}'
        # Call to send_email function
        utilities.send_email(recipient, subject, body, message)