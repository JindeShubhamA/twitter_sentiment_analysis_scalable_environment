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

query = {"query": {"match_all": {}}}