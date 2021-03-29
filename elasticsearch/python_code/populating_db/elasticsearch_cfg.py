from elasticsearch import Elasticsearch

# connect to elastic
es = Elasticsearch(hosts=['elasticsearch:9200'])
print('Connected to the ES cluster.')

# index settings
settings_body = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 1,
    }
}

# raw tweets mapping
raw_mapping = {
    "properties": {
        "user_id": {"type": "keyword"},
        "tweet_id": {"type": "keyword"},
        "tweet": {"type": "text"},
        "timestamp": {"type": "date"},
        "location": {"type": "geo_point"}
    }
}

# length mapping
length_mapping = {
    "properties": {
        "tweet_length": {"type": "integer"},
        "count": {"type": "integer"},
        "total_sentiment": {"type": "float"},
        "absolute_sentiment": {"type": "float"},
        "mean_sentiment": {"type": "float"}
    }
}

# state mapping
state_mapping = {
    "properties": {
        "state_name": {"type": "keyword"},
        "state_code": {"type": "keyword"},
        "count": {"type": "integer",},
        "total_sentiment": {"type": "float"},
        "absolute_sentiment": {"type": "float"},
        "mean_sentiment": {"type": "float"}
    }
}

# day mapping
day_mapping = {
    "properties": {
        "day_name": {"type": "keyword"},
        "day_number": {"type": "integer"},
        "count": {"type": "integer"},
        "total_sentiment": {"type": "float"},
        "absolute_sentiment": {"type": "float"},
        "mean_sentiment": {"type": "float"}
    }
}

# hour mapping
hour_mapping = {
    "properties": {
        "hour": {"type": "integer"},
        "count": {"type": "integer"},
        "total_sentiment": {"type": "float"},
        "absolute_sentiment": {"type": "float"},
        "mean_sentiment": {"type": "float"}
    }
}

# create the raw tweets inex
print('Creating raw tweets index...')
es.indices.create(index='tweets', body=settings_body, ignore=400)
print('Done!')

print('Updating mapping...')
es.indices.put_mapping(body=raw_mapping, index='tweets')
print('Done!')


index_names = ['length', 'state', 'day', 'hour']

for name in index_names:
    print(f'Creating {name} reduced tweets index...')
    temp = name + '_reduced'
    es.indices.create(index=temp, body=settings_body, ignore=400)
    print('Done!')

    map_body = name + '_mapping'
    print('Updating mapping...')
    es.indices.put_mapping(body=eval(map_body), index=temp)
    print('Done!')

print('Created all indexes!')


