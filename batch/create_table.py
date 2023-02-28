import mysql.connector
import json
import logging

# Get configuration data
with open('/tmp/pycharm_project_4/config.json') as f:
    config = json.load(f)

# Connect to MySQL database
db = mysql.connector.connect(
    host="localhost",
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database="stocks"
)

# Create a cursor object
cursor = db.cursor()

# Execute the SQL statement to create the table
cursor.execute("""
  CREATE TABLE IF NOT EXISTS daily_stocks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10), 
    volume FLOAT,
    volume_weighted FLOAT,
    open_price FLOAT,
    close_price FLOAT,
    highest_price FLOAT,
    lowest_price FLOAT,
    date TIMESTAMP,
    transactions_number INT
  )
""")

# Close the database connection
db.close()