import pyspark
from pyspark.sql import DataFrame, GroupedData
from pyspark.sql.functions import split
from pyspark.sql.types import ArrayType, StringType

import settings
import os
from reverse_geocoder import ReverseGeocoder
from search_tree import SearchTree
from tweet_processing import *


class SparkDriver(object):

    def __init__(self):
        # determine if we are in kubernetes or not
        kube_mode = os.environ.get(settings.kube_mode_check) == "true"
        # create spark session by creating a config first
        self.spark_conf = pyspark.SparkConf()
        self.spark_conf.setAll(settings.spark_settings)

        # if we are not inside kubernetes, that means we likely won't have access to the elasticsearch nodes
        # so we change some settings that allow us to communicate with elasticsearch across different networks
        if not kube_mode:
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
        print(f"Spark driver is in {'kubernetes' if kube_mode else 'local'} mode")
        print(f"Running PySpark version {self.spark_session.version}")


    def read_es(self):
        # for now the query is just static, but this could be updated in a loop for example
        # q = """{
        #   "query": {
        #     "bool": {
        #       "must": [
        #         { "match": { "user_id": "36229248" }}
        #       ]
        #     }
        #   }
        # }"""

        q = """{
          "query": {
            "match_all": {}
          }
        }"""

        print("Getting tweets from Elasticsearch...")

        retrieved_tweets = self.spark_session.read.format("es")\
            .option("es.nodes", settings.es_cluster_settings["es.nodes"])\
            .option("es.port", settings.es_cluster_settings["es.port"])\
            .option("es.query", q)\
            .load(settings.es_resource_names["read_resource"])

        print(f"Got {retrieved_tweets.count()} tweets")

        return retrieved_tweets


    def process_tweets(self, tweets: DataFrame) -> DataFrame:
        tweets.show()

        with_sentiment = self.add_sentiment(tweets)
        with_date_and_time = self.add_time(with_sentiment)

        by_state = self.group_by_state(with_sentiment)
        by_hour = self.group_by_hour(with_date_and_time)
        by_day_of_week = self.group_by_day_of_week(with_date_and_time)

        print("reduced by state:")
        by_state.show(51)

        print("reduced by hour:")
        by_hour.show(24)

        print("reduced by day of week:")
        by_day_of_week.show()

        return by_state


    def add_sentiment(self, tweets: DataFrame) -> DataFrame:
        # to each row (aka tweet), add a column that contains the sentiment of that tweet
        return tweets\
            .withColumn("tweet_sentiment", get_sentiment_udf("tweet"))


    def add_time(self, tweets: DataFrame) -> DataFrame:
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
        return to_be_agg\
            .agg(F.count(count_col).alias("count"),
                 F.sum("tweet_sentiment").alias("total_sentiment"),
                 F.sum(F.abs(F.col("tweet_sentiment"))).alias("absolute_sentiment"),
                 F.mean("tweet_sentiment").alias("mean_sentiment"))


    def group_by_state(self, tweets_with_stats: DataFrame, search_tree: SearchTree=None) -> DataFrame:
        # group the new dataframe rows by location (at this point still in coordinates!)
        location_reduced = self.agg_stats(tweets_with_stats.groupBy("location"), "location")

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

        state_tweets = location_reduced\
            .withColumn("state", get_state_udf("location"))\
            .groupBy("state")\
            .agg(F.sum("count").alias("count"),
                 F.sum("total_sentiment").alias("total_sentiment"),
                 F.sum(F.abs(F.col("absolute_sentiment"))).alias("absolute_sentiment"))\
            .withColumn("mean_sentiment", F.col("total_sentiment") / F.col("count"))\
            .withColumn("state_name", F.col("state").getItem(0))\
            .withColumn("state_code", F.col("state").getItem(1))\
            .drop("state")

        return state_tweets.select("state_name", "state_code", "count", "total_sentiment", "absolute_sentiment", "mean_sentiment")


    def group_by_hour(self, tweets_with_date_and_time: DataFrame) -> DataFrame:
        # reduce by hour, and order them by ascending hour
        hour_reduced = self.agg_stats(tweets_with_date_and_time\
            .groupBy("timestamp_hour"), "timestamp_hour")\
            .withColumnRenamed("timestamp_hour", "hour")\
            .orderBy("hour")

        return hour_reduced.select("hour", "count", "total_sentiment", "absolute_sentiment", "mean_sentiment")


    def group_by_day_of_week(self, tweets_with_date_and_time: DataFrame) -> DataFrame:
        # reduce by day of week, and order them by ascending day
        dow_reduced = self.agg_stats(tweets_with_date_and_time\
            .groupBy("timestamp_day_of_week"), "timestamp_day_of_week")\
            .withColumn("day_name", get_day_of_week(F.col("timestamp_day_of_week")))\
            .withColumnRenamed("timestamp_day_of_week", "day_number")\
            .orderBy("day_number")

        return dow_reduced.select("day_name", "day_number", "count", "total_sentiment", "absolute_sentiment", "mean_sentiment")


    def store_processed_tweets(self, processed_tweets: DataFrame) -> None:
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
    p_tweets = spark_driver.process_tweets(r_tweets)
    # spark_driver.store_processed_tweets(p_tweets)
