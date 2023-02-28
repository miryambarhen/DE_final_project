from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["stocks_db"]
col = db["articles"]

# show the data
for x in col.find():
    print(x)
