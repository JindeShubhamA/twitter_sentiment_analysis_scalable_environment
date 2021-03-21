import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import split


import settings
import os
from reverse_geocoder import ReverseGeocoder
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
        q = """{
          "query": {
            "bool": {
              "must": [
                { "match": { "user_id": "36229248" }}
              ]
            }
          }
        }"""

        # q = """{
        #   "query": {
        #     "match_all": {}
        #   }
        # }"""

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

        with_stats = self.add_stats(tweets)
        by_location = self.group_by_state(with_stats)
        by_hour = self.group_by_hour(with_stats)

        print("reduced by hour:")
        by_hour.show(24)

        print("reduced by state:")
        by_location.show(51)

        return by_location


    def add_stats(self, tweets):
        # to each row (aka tweet), add a column that contains the sentiment of that tweet
        with_sentiment = tweets \
            .withColumn("tweet_sentiment", get_sentiment_udf("tweet"))

        mean_sentiment = with_sentiment.groupBy().agg(F.mean("tweet_sentiment").alias("mean")).take(1)[0]["mean"]

        # to each row, add a column that contains the mean sentiment
        # and the deviation of that row's sentiment from the mean
        with_deviation = with_sentiment \
            .withColumn("mean_sentiment", F.lit(mean_sentiment)) \
            .withColumn("deviation", F.abs(F.col("tweet_sentiment") - F.col("mean_sentiment")))

        return with_deviation


    def group_by_state(self, tweets_with_stats, search_tree=None):
        # group the new dataframe rows by location (at this point still in coordinates!)
        location_reduced = tweets_with_stats \
            .groupBy("location") \
            .agg(F.count("location").alias("weight"),
                 F.sum("tweet_sentiment").alias("total_sentiment"),
                 F.sum("deviation").alias("total_deviation")) \
            .withColumn("mean_sentiment", F.col("total_sentiment") / F.col("weight")) \
            .withColumn("standard_deviation", F.col("total_deviation") / F.col("weight"))

        # create a search tree and broadcast it to the workers
        tree = ReverseGeocoder.create_tree() if search_tree is None else search_tree
        btree = self.spark_session.sparkContext.broadcast(tree)
        broadcasted_tree = btree.value

        # do reverse geocoding to get the 'average' sentiment per state
        get_state_udf = F.udf(
            lambda z: ReverseGeocoder.get_from_tree_by_string(z, broadcasted_tree).record
        )
        state_tweets = location_reduced.withColumn("state", get_state_udf("location")) \
            .groupBy("state") \
            .agg(F.sum("weight").alias("weight"),
                 F.sum("total_sentiment").alias("state_sentiment"),
                 F.sum("total_deviation").alias("state_deviation")) \
            .withColumn("mean_sentiment", F.col("state_sentiment") / F.col("weight")) \
            .withColumn("standard_deviation", F.col("state_deviation") / F.col("weight"))

        return state_tweets


    def group_by_hour(self, tweets_with_stats):
        # do grouping by time
        with_date_and_time = tweets_with_stats.withColumn("timestamp_year", F.year("timestamp")) \
            .withColumn("timestamp_month", F.month("timestamp")) \
            .withColumn("timestamp_day", F.dayofmonth("timestamp")) \
            .withColumn("timestamp_hour", F.hour("timestamp"))

        # reduce by hour, and order them by ascending hour
        hour_reduced = with_date_and_time \
            .groupBy("timestamp_hour") \
            .agg(F.count("location").alias("weight"),
                 F.sum("tweet_sentiment").alias("total_sentiment"),
                 F.sum("deviation").alias("total_deviation")) \
            .withColumn("mean_sentiment", F.col("total_sentiment") / F.col("weight")) \
            .withColumn("standard_deviation", F.col("total_deviation") / F.col("weight")) \
            .orderBy("timestamp_hour")

        return hour_reduced


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
