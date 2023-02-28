from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client["stocks_db"]
col = db["tickers"]

tickers_list = [
    {"_id": "1", "ticker": "AAPL", "name": "Apple"},
    {"_id": "2", "ticker": "MSFT", "name": "Microsoft"},
    {"_id": "3", "ticker": "AMZN", "name": "Amazon"},
    {"_id": "4", "ticker": "BRK - B", "name": "Berkshire"},
    {"_id": "5", "ticker": "TSLA", "name": "Tesla"},
    {"_id": "6", "ticker": "NVDA", "name": "Nvidia"},
    {"_id": "7", "ticker": "V", "name": "Visa"},
    {"_id": "8", "ticker": "UNH", "name": "UnitedHealth"},
    {"_id": "9", "ticker": "XOM", "name": "Exxon"},
    {"_id": "10", "ticker": "META", "name": "Meta Platform"}
]

# col.insert_many(tickers_list)

# show the data
for x in col.find():
    print(x)
