from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv
from elasticsearch.helpers import scan
from es import settings

class esDriver(object):
    def __init__(self):
        self.es = Elasticsearch(hosts=['http://elasticsearch:9200'])

    def create(self):
        print('Creating tweets index...')
        self.es.indices.create(index='tweets', body=settings.settings_body, ignore=400)
        print('Done!')

    def populate(self, csvFile): # find all
        with open(csvFile, newline='\n') as f:
            reader = csv.DictReader(f, delimiter='\t')
            print("here")
            bulk(self.es, reader, index='tweets', raise_on_error=False)  # skips rows with bad timestamps

    def scanAll(self):
        es_response = scan(
            self.es,
            index='tweets',
            query={"query": {"match_all": {}}}
        )
        return es_response