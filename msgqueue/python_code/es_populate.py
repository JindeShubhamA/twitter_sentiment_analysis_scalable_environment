from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv

es = Elasticsearch(hosts=['http://elasticsearch:9200/'], timeout=120)

settings_body = {
    "settings": {
        "number_of_shards": 5,
        "number_of_replicas": 1,
        "analysis": {
            "filter": {
                "stemmer": {
                    "type": "stemmer",
                    "language": "english"
                },
                "stopwords": {
                    "type": "stop",
                    "stopwords": ["_english_"]
                }
            },
            "analyzer": {
                "tweet_analyzer": {
                    "filter": ["lowercase", "stemmer", "stopwords"],
                    "type": "custom",
                    "tokenizer": "standard"
                }
            }
        }
    }
}

mappings_body = {
    "properties": {
        "user_id": {"type": "keyword"},
        "tweet_id": {"type": "keyword"},
        "tweet": {"type": "text", "analyzer":"tweet_analyzer", "search_analyzer":"tweet_analyzer"},
        "timestamp": {"type": "date"},
        "location": {"type": "geo_point"}
    }
}

print('Creating tweets index...')
es.indices.create(index='tweets', body=settings_body, ignore=400, timeout=300)
print('Done!')

print('Updating mapping...')
es.indices.put_mapping(body=mappings_body, index='tweets')
print('Done!')


print('Populating database...')
with open('tweet_data_500k.csv', newline='\n') as f:
    reader = csv.DictReader(f, delimiter='\t')
    bulk(es, reader, index='tweets', raise_on_error=False) # skips rows with bad timestamps

print('Done!')