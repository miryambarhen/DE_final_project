from pymongo import MongoClient

client = MongoClient('localhost', 27017)

db = client["stocks_db"]
col = db["users"]

# Show users
for x in col.find():
    print(x)
