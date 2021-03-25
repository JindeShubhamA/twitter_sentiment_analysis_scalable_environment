from typing import Tuple

import pyspark
from pyspark.sql import DataFrame, GroupedData
from pyspark.sql.types import ArrayType, StringType

from settings import IN_KUBE_MODE
import settings
import math
from reverse_geocoder import ReverseGeocoder
from search_tree import SearchTree
from tweet_processing import *

# the maximum amount of characters a tweet can contain
MAX_TWEET_CHARS = 280
# the size of the chunks we want to divide the lengths in.
# so we get 0-chunk_size, chunk_size-chunk_size*2, chunk_size*2-chunk_size*3, etc.
LENGTH_CHUNK_SIZE = 10
NUM_CHUNKS = math.ceil(MAX_TWEET_CHARS / LENGTH_CHUNK_SIZE)

# if set to true, will print more details along the way to help with debugging
DEBUG = True


class SparkDriver(object):

    def __init__(self):
        # create spark session by creating a config first
        self.spark_conf = pyspark.SparkConf()
        self.spark_conf.setAll(settings.spark_settings)

        # if we are not inside kubernetes, that means we likely won't have access to the elasticsearch nodes
        # so we change some settings that allow us to communicate with elasticsearch across different networks
        if not IN_KUBE_MODE:
            self.spark_conf.set("spark.es.nodes.discovery", False)
            self.spark_conf.set("spark.es.nodes.wan.only", True)

        session_builder = pyspark.sql.SparkSession.builder
        session_builder.config(conf = self.spark_conf)

        # create the actual spark session
        self.spark_session = session_builder.getOrCreate()

        # add the files with the custom classes so the workers have access to them as well
        self.spark_session.sparkContext.addPyFile("reverse_geocoder.py")
        self.spark_session.sparkContext.addPyFile("geocoding_result.py")
        self.spark_session.sparkContext.addPyFile("search_tree.py")
        self.spark_session.sparkContext.addPyFile("tweet_processing.py")

        # print some helpful information
        print("Configured Spark and Spark driver")
        print(f"Spark driver is in {'kubernetes' if IN_KUBE_MODE else 'local'} mode")
        print(f"Running PySpark version {self.spark_session.version}")


    def read_es(self) -> DataFrame:
        """reads a set of tweets from elasticsearch

        the relevant settings from `settings.py` are used for the configuration

        :return: the retrieved tweets
        :rtype: DataFrame
        """

        # this is our test query, with a randomly selected user id to get tweets from
        # this user has around 177 tweets, so it is a nice test sample
        q = """{
          "query": {
            "bool": {
              "must": [
                { "match": { "user_id": "36229248" }}
              ]
            }
          }
        }"""

        # this is our query to get the full database
        # q = """{
        #   "query": {
        #     "match_all": {}
        #   }
        # }"""

        print("Getting tweets from ElasticSearch...")

        retrieved_tweets = self.spark_session.read.format("es")\
            .option("es.nodes", settings.es_cluster_settings["es.nodes"])\
            .option("es.port", settings.es_cluster_settings["es.port"])\
            .option("es.query", q)\
            .load(settings.es_resource_names["read_resource"])

        print(f"Got {retrieved_tweets.count()} tweets")

        return retrieved_tweets


    def process_tweets(self, tweets: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        """executes all processing steps on the tweets from elasticsearch

        | these steps are:
        | - adding the sentiment
        | - adding the timestamp component columns
        | - grouping by US state
        | - grouping by hour of day
        | - grouping by day of the week
        | - grouping by length of the tweet

        :param tweets: the tweets from elasticsearch
        :type tweets: DataFrame

        :return: all of the results from the grouping steps, in the order that they are mentioned above
        :rtype: Tuple[DataFrame, DataFrame, DataFrame, DataFrame]
        """

        if DEBUG:
            tweets.show()

        with_sentiment = self.add_sentiment(tweets)
        with_date_and_time = self.add_time(with_sentiment)

        by_state = self.group_by_state(with_sentiment)
        by_hour = self.group_by_hour(with_date_and_time)
        by_day_of_week = self.group_by_day_of_week(with_date_and_time)
        by_length = self.group_by_length(with_sentiment)

        if DEBUG:
            print("reduced by US state:")
            by_state.show(51)

            print("reduced by hour of day:")
            by_hour.show(24)

            print("reduced by day of week:")
            by_day_of_week.show()

            print("reduced by length of tweet:")
            by_length.show(NUM_CHUNKS)

        return by_state, by_hour, by_day_of_week, by_length


    def add_sentiment(self, tweets: DataFrame) -> DataFrame:
        """adds a column containing the sentiment of each tweet

        :param tweets: the tweets from elasticsearch
        :type tweets: DataFrame

        :return: the `tweets` with the sentiments added
        :rtype: DataFrame
        """

        # to each row (aka tweet), add a column that contains the sentiment of that tweet
        return tweets\
            .withColumn("tweet_sentiment", get_sentiment_udf("tweet"))


    def add_time(self, tweets: DataFrame) -> DataFrame:
        """splits the timestamp column into its individual components and adds those columns to the dataframe

        | those columns are:
        | - `timestamp_year`: the year of the timestamp
        | - `timestamp_month`: the month of the timestamp
        | - `timestamp_day`: the day of the month of the timestamp
        | - `timestamp_hour`: the hour of day of the timestamp
        | - `timestamp_day_of_week`: the day of week of the timestamp
        | note: adding more columns would be trivial to do, if desired

        :param tweets: the tweets from elasticsearch
        :type tweets: DataFrame

        :return: the `tweets` with the timestamp component columns added
        :rtype: DataFrame
        """

        # do grouping by time
        return tweets\
            .withColumn("timestamp_year", F.year("timestamp"))\
            .withColumn("timestamp_month", F.month("timestamp"))\
            .withColumn("timestamp_day", F.dayofmonth("timestamp"))\
            .withColumn("timestamp_hour", F.hour("timestamp"))\
            .withColumn("timestamp_day_of_week", (F.dayofweek("timestamp") + 5) % 7)
            # (x + 5) % 7 in the line above is short for (x - 2 + 7) % 7,
            # we do -1 to index from 0, then another -1 to wrap Sunday around to the end of the list


    def agg_stats(self, to_be_agg: GroupedData, count_col: str) -> DataFrame:
        """takes a dataframe that has `groupBy` called on it and aggregates it with columns for the statistics

        | those columns are:
        | - `count`: the amount of items that belong to the group in that row
        | - `total_sentiment`: the sum of the sentiments in the group in that row
        | - `absolute_sentiment`: the sum of the absolute values of the sentiments in the group in that row,
            this is useful to determine how spread out the sentiments were
        | - `mean_sentiment`: the average sentiment of the group in that row

        :param to_be_agg: the data to be aggregated
        :type to_be_agg: GroupedData
        :param count_col: the column that should be counted to get the amount of items in a group,
            this is usually equal to the column that the dataframe was grouped by
        :type count_col: str

        :return: the aggregated dataframe
        :rtype: DataFrame
        """

        return to_be_agg\
            .agg(F.count(count_col).alias("count"),
                 F.sum("tweet_sentiment").alias("total_sentiment"),
                 F.sum(F.abs(F.col("tweet_sentiment"))).alias("absolute_sentiment"),
                 F.mean("tweet_sentiment").alias("mean_sentiment"))


    def group_by_state(self, tweets_with_sentiment: DataFrame, search_tree: SearchTree=None) -> DataFrame:
        """groups the tweets by the US state they were posted from

        to go from coordinates to a US state, we use the functions from the `reverse_geocoder.py` module.
        this, in turn, uses the shapefiles from the `shapefiles` directory
        to find the state that contains the coordinates

        :param tweets_with_sentiment: the tweets from elasticsearch with a sentiment column added
        :type tweets_with_sentiment: DataFrame
        :param search_tree: the search tree to use, can be left out to generate a new tree
        :type search_tree: SearchTree, optional

        :return: the `tweets_with_sentiment`, grouped by the US state they were posted from
        :rtype: DataFrame
        """

        # group the new dataframe rows by location (at this point still in coordinates!)
        location_reduced = self.agg_stats(tweets_with_sentiment.groupBy("location"), "location")

        # create a search tree and broadcast it to the workers
        tree = ReverseGeocoder.create_tree() if search_tree is None else search_tree
        btree = self.spark_session.sparkContext.broadcast(tree)
        broadcasted_tree = btree.value

        # do reverse geocoding to get the 'average' sentiment per state
        # also make sure to specify the return type (ArrayType(StringType()))
        # as this helps spark to keep it as an array instead of converting it to a string
        get_state_udf = F.udf(
            lambda z: ReverseGeocoder.get_from_tree_by_string(z, broadcasted_tree).record,
            ArrayType(StringType())
        )

        return location_reduced\
            .withColumn("state", get_state_udf("location"))\
            .groupBy("state")\
            .agg(F.sum("count").alias("count"),
                 F.sum("total_sentiment").alias("total_sentiment"),
                 F.sum(F.abs(F.col("absolute_sentiment"))).alias("absolute_sentiment"))\
            .withColumn("mean_sentiment", F.col("total_sentiment") / F.col("count"))\
            .withColumn("state_name", F.col("state").getItem(0))\
            .withColumn("state_code", F.col("state").getItem(1))\
            .select("state_name", "state_code", "count", "total_sentiment", "absolute_sentiment", "mean_sentiment")


    def group_by_hour(self, tweets_with_date_and_time: DataFrame) -> DataFrame:
        """groups the tweet by the hour of day they were posted

        | this is achieved by simply truncating the timestamp:
        | - ex: 14:10:00 gets grouped in hour 14
        | - ex: 20:50:00 gets grouped in hour 20

        :param tweets_with_date_and_time: the tweets from elasticsearch with extra columns
            that separate the timestamp into its individual components
        :type tweets_with_date_and_time: DataFrame

        :return: the `tweets_with_date_and_time`, grouped by hour of day,
            and ordered by those as well
        :rtype: DataFrame
        """

        # reduce by hour, and order them by ascending hour
        return self.agg_stats(tweets_with_date_and_time\
            .groupBy("timestamp_hour"), "timestamp_hour")\
            .withColumnRenamed("timestamp_hour", "hour")\
            .orderBy("hour")\
            .select("hour", "count", "total_sentiment", "absolute_sentiment", "mean_sentiment")


    def group_by_day_of_week(self, tweets_with_date_and_time: DataFrame) -> DataFrame:
        """groups the tweets by the day of the week they were posted

        :param tweets_with_date_and_time: the tweets from elasticsearch with extra columns
            that separate the timestamp into its individual components
        :type tweets_with_date_and_time: DataFrame

        :return: the `tweets_with_date_and_time`, grouped by day of the week,
            and ordered by those as well (starting at Monday)
        :rtype: DataFrame
        """

        # reduce by day of week, and order them by ascending day
        return self.agg_stats(tweets_with_date_and_time\
            .groupBy("timestamp_day_of_week"), "timestamp_day_of_week")\
            .withColumn("day_name", get_day_of_week(F.col("timestamp_day_of_week")))\
            .withColumnRenamed("timestamp_day_of_week", "day_number")\
            .orderBy("day_number")\
            .select("day_name", "day_number", "count", "total_sentiment", "absolute_sentiment", "mean_sentiment")


    def group_by_length(self, tweets_with_sentiment: DataFrame) -> DataFrame:
        """groups the tweets by their length

        this functions uses the constant LENGTH_CHUNK_SIZE to determine the size and the ranges of the groups

        :param tweets_with_sentiment: the tweets from elasticsearch with a sentiment column added
        :type tweets_with_sentiment: DataFrame

        :return: the `tweets_with_sentiment`, grouped by length of the tweet
        :rtype: DataFrame
        """

        if DEBUG:
            print(f"Tweets will be grouped by multiples of {LENGTH_CHUNK_SIZE}")

        # reduce by length of tweet
        return self.agg_stats(tweets_with_sentiment\
            .withColumn("tweet_length", F.floor(F.length("tweet") / LENGTH_CHUNK_SIZE) * LENGTH_CHUNK_SIZE)\
            .groupBy("tweet_length"), "tweet_length")\
            .orderBy("tweet_length")


    def store_processed_tweets(self, processed_tweets: DataFrame) -> None:
        """stores the processed tweets back in elasticsearch

        the relevant setting from `settings.py` are used for the configuration

        :param processed_tweets: the dataframe to store in elasticsearch, which contains the processed data
        :type processed_tweets: DataFrame

        :return: None
        """

        print("Writing to es cluster...", processed_tweets)
        # write to elasticsearch on the set ip (node), port, and index (resource)
        processed_tweets.write.format("es")\
            .option("es.nodes", settings.es_cluster_settings["es.nodes"])\
            .option("es.port", settings.es_cluster_settings["es.port"])\
            .option("es.resource", settings.es_resource_names["write_resource"]) \
            .option("es.write.operation", settings.es_cluster_settings["es.write.operation"])\
            .option("es.mapping.id", settings.es_cluster_settings["es.mapping.id"])\
            .mode(settings.es_cluster_settings["mode"])\
            .save()

        print("Done!")


if __name__ == "__main__":
    spark_driver = SparkDriver()

    r_tweets = spark_driver.read_es()
    p_by_state, p_by_hour, p_by_day_of_week, p_by_length = spark_driver.process_tweets(r_tweets)
    # spark_driver.store_processed_tweets(p_by_state)
    # spark_driver.store_processed_tweets(p_by_hour)
    # spark_driver.store_processed_tweets(p_by_day_of_week)
    # spark_driver.store_processed_tweets(p_by_length)
