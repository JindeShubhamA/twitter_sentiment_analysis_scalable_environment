from elasticsearch import Elasticsearch, exceptions
from elasticsearch.helpers import bulk
import csv
from elasticsearch.helpers import scan
from es import settings
import json

class esDriver(object):
    def __init__(self):
        self.es = Elasticsearch(hosts=['http://elasticsearch:9200/'])


    def create(self):
        print('Creating tweets index...')
        self.es.indices.create(index='tweets', body=settings.settings_body, ignore=400)


    def populate(self, csvFile): # find all
        with open(csvFile, newline='\n') as f:
            reader = csv.DictReader(f, delimiter='\t')

            bulk(self.es, reader, index='tweets', raise_on_error=False)  # skips rows with bad timestamps

    def scanAll(self):
        query = settings.query
        try:
            # result = Elasticsearch.explain(self.es, index="tweets", id=12345, body=query)
            # print(json.dumps(result, indent=4))
            res = self.es.search(index="tweets", doc_type="_doc", body={
                'size': 100,
                'query': {
                    'match_all': {}
                }
            })
            ### Adding file write for debugging. Will be removed afterwards
            f = open("demofile3.txt", "a+")
            f.write(str(res['hits']['total']))
            data = [doc for doc in res['hits']['hits']]
            # for doc in data:
            #     print(str(doc))
            #     f.write(str(doc))
            f.close()
        except exceptions.NotFoundError as error:
            f = open("demofile3.txt", "w+")
            f.write(str(error))
            f.close()
            print(type(error))

        return data