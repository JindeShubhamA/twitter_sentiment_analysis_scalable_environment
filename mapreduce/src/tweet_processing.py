import re
from textblob import TextBlob
from pyspark.sql import functions as F
from reverse_geocoder import ReverseGeocoder


def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w+:\ / \ / \S+)", " ", tweet).split())


def get_tweet_sentiment(tweet):
    analysis = TextBlob(clean_tweet(tweet))
    return analysis.sentiment.polarity


# UDFs needed for DataFrame map/reduce
get_sentiment_udf = F.udf(
    lambda x: float(get_tweet_sentiment(x))
)

# get_state_udf = F.udf(
#     lambda x: ReverseGeocoder.get_from_tree_by_string(x, broadcasted_tree)
# )

# TODO: find a way to do this without udf, since those apparently kill parallelism
days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
get_day_of_week = F.udf(lambda z: days[z])