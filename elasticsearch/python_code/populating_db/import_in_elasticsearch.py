from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from collections import deque #needed for parallel_bulk
import csv

es = Elasticsearch(hosts=['elasticsearch:9200'])

print('Populating database...')
with open('tweet_data_500k.csv', newline='\n') as f:
    reader = csv.DictReader(f, delimiter='\t')
    # bulk(es, reader, index='tweets', raise_on_error=False) # skips rows with bad timestamps
    deque(parallel_bulk(es, reader, index='tweets', raise_on_error=False, thread_count=6), maxlen=0)

print('Done!')