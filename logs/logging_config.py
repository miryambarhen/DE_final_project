import datetime
import logging
from elasticsearch import Elasticsearch
from datetime import datetime

# Set up Elasticsearch connection
es = Elasticsearch()

# Create logger with the desired format
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(topic)s - %(message)s')
file_handler = logging.FileHandler('/tmp/pycharm_project_4/logs/my_logs.log')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Define a function for sending logs to Elasticsearch
def send_to_elasticsearch(log_dict):
    es.index(index='logs', doc_type='_doc', body=log_dict)

# Write to logger and Elasticsearch
def write_to_log(topic, message, level=logging.INFO):
    logger.log(level, message, extra={'topic': topic})

    # Send the log to Elasticsearch
    log_dict = {
        'timestamp': datetime.now(),
        'level': logging.getLevelName(level),
        'topic': topic,
        'message': message
    }
    send_to_elasticsearch(log_dict)
