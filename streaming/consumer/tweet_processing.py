import re
# import textblob
# from textblob import TextBlob
from pyspark.sql import functions as F
# from reverse_geocoder import ReverseGeocoder


def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w+:\ / \ / \S+)", " ", tweet).split())


def get_tweet_sentiment(tweet):
    analysis = TextBlob(clean_tweet(tweet))
    return analysis



