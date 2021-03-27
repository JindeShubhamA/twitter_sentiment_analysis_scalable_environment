from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from collections import deque #needed for parallel_bulk
import csv

es = Elasticsearch(hosts=['elasticsearch:9200'])

# settings_body = {
#     "settings": {
#         "number_of_shards": 3,
#         "number_of_replicas": 1,
#         "analysis": {
#             "filter": {
#                 "stemmer": {
#                     "type": "stemmer",
#                     "language": "english"
#                 },
#                 "stopwords": {
#                     "type": "stop",
#                     "stopwords": ["_english_"]
#                 }
#             },
#             "analyzer": {
#                 "tweet_analyzer": {
#                     "filter": ["lowercase", "stemmer", "stopwords"],
#                     "type": "custom",
#                     "tokenizer": "standard"
#                 }
#             }
#         }
#     }
# }

# mappings_body = {
#     "properties": {
#         "user_id": {"type": "keyword"},
#         "tweet_id": {"type": "keyword"},
#         "tweet": {"type": "text", "analyzer":"tweet_analyzer", "search_analyzer":"tweet_analyzer"},
#         "timestamp": {"type": "date"},
#         "location": {"type": "geo_point"}
#     }
# }

print('Populating database...')
with open('tweet_data_500k.csv', newline='\n') as f:
    reader = csv.DictReader(f, delimiter='\t')
    # bulk(es, reader, index='tweets', raise_on_error=False) # skips rows with bad timestamps
    deque(parallel_bulk(es, reader, index='tweets', raise_on_error=False, thread_count=6), maxlen=0)

print('Done!')