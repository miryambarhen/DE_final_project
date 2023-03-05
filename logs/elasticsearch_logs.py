from elasticsearch import Elasticsearch

# create an Elasticsearch client
es_client = Elasticsearch()

# search for all log messages with level INFO
search_results = es_client.search(
    index='logs',
    body={
        'query': {
            'match': {
                'level': 'INFO'
            }
        }
    }
)

# print each log message
for hit in search_results['hits']['hits']:
    print(hit['_source']['timestamp'], hit['_source']['level'], hit['_source']['message'])
