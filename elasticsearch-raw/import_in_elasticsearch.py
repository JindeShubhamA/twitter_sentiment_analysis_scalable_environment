from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv

es = Elasticsearch()

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
                    "filter": ["stopwords", "lowercase", "stemmer"],
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
        "tweet": {"type": "text"},
        "timestamp": {"type": "date"},
        "location": {"type": "geo_point"}
    }
}

print('Creating tweets index...')
es.indices.create(index='tweets')
print('Done!')

# # Test this tomorrow !!
# print('Updating settings...')
# es.indices.put_settings(index_cfg_body, index='tweets')
# print('Done!')

print('Updating mapping...')
es.indices.put_mapping(index_cfg_body, index='tweets')
print('Done!')


print('Populating database...')
with open('data/tweet_data_500k.csv', newline='\n') as f:
    reader = csv.DictReader(f, delimiter='\t')
    bulk(es, reader, index='tweets', raise_on_error=False) # skips rows with bad timestamps

print('Done!')