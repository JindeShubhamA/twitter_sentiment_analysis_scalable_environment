from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv

es = Elasticsearch()

with open('data/test_data.csv',newline='\n') as f:
    reader = csv.DictReader(f, delimiter=';')
    bulk(es, reader, index='test_user')