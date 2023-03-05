import logging
import os
import uuid
from datetime import datetime
from elasticsearch import Elasticsearch


def setup_logging(topic=None):

    log_dir = '/tmp/pycharm_project_4/logs'

    # create a formatter that includes the date and time
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(topic)s - %(message)s')

    # set the log level for the "requests" logger to WARNING
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # set the log level for the root logger to INFO
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger()

    # create a console handler with level INFO and the formatter
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # create a file handler with level DEBUG and the formatter
    log_filename = 'logger.log'
    if log_dir is not None:
        log_filename = os.path.join(log_dir, log_filename)
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # create an Elasticsearch handler with level INFO and the formatter
    es_handler = ElasticsearchHandler(topic)
    es_handler.setLevel(logging.DEBUG)
    es_handler.setFormatter(formatter)
    logger.addHandler(es_handler)

    return logger

class ElasticsearchHandler(logging.Handler):
    def __init__(self, topic=None):
        super().__init__()
        self.topic = topic
        self.es_client = Elasticsearch(hosts=['localhost'])

    def emit(self, record):
        log_entry = {
            # 'uuid': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            # 'logger': record.name,
            # 'path': record.pathname,
            # 'lineno': record.lineno,
            # 'funcName': record.funcName,
            'topic': self.topic
        }
        self.es_client.index(index='logs', doc_type='_doc', body=log_entry)