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

        # to each row (aka tweet), add a column that contains the sentiment of that tweet
        with_sentiment = tweets\
            .withColumn("tweet_sentiment", get_sentiment_udf("tweet"))

        mean_sentiment = with_sentiment.groupBy().agg(F.mean("tweet_sentiment").alias("mean")).take(1)[0]["mean"]

        with_deviation = with_sentiment\
            .withColumn("mean_sentiment", F.lit(mean_sentiment))\
            .withColumn("deviation", F.abs(F.col("tweet_sentiment") - F.col("mean_sentiment")))

        with_deviation.show()

        # group the new dataframe rows by location (at this point still in coordinates!)
        location_reduced = with_deviation\
            .groupBy("location")\
            .agg(F.count("location").alias("weight"),
                 F.sum("tweet_sentiment").alias("total_sentiment"),
                 F.sum("deviation").alias("total_deviation"))\
            .withColumn("average_sentiment", F.col("total_sentiment") / F.col("weight"))\
            .withColumn("average_deviation", F.col("total_deviation") / F.col("weight"))

        # do grouping by time
        # first split a timestamp of the format (yyyy-mm-dd HH:mm:ss) into the date (yyyy-mm-dd) and time (HH:mm:ss)
        split_timestamp = split(with_deviation["timestamp"], " ")
        # create a new intermediate dataframe with those columns added
        with_st = with_deviation.withColumn("timestamp_date", split_timestamp.getItem(0))\
                                .withColumn("timestamp_time", split_timestamp.getItem(1))
        # split those new columns into year, month, day, hour columns
        # (minutes and seconds seem kinda useless but are trivial to add as well)
        split_date = split(with_st["timestamp_date"], "-")
        split_time = split(with_st["timestamp_time"], ":")
        # add the columns to new dataframe
        with_date_and_time = with_st.withColumn("timestamp_year", split_date.getItem(0))\
                                    .withColumn("timestamp_month", split_date.getItem(1))\
                                    .withColumn("timestamp_day", split_date.getItem(2))\
                                    .withColumn("timestamp_hour", split_time.getItem(0))
        # reduce by hour, and order them by ascending hour
        hour_reduced = with_date_and_time\
            .groupBy("timestamp_hour")\
            .agg(F.count("location").alias("weight"),
                 F.sum("tweet_sentiment").alias("total_sentiment"),
                 F.sum("deviation").alias("total_deviation"))\
            .withColumn("mean_sentiment", F.col("total_sentiment") / F.col("weight"))\
            .withColumn("standard_deviation", F.col("total_deviation") / F.col("weight"))\
            .orderBy("timestamp_hour")
        # count = location_reduced.count()

        # print("1st mapreduce: ")
        # print(f"{count} rows")
        # print(location_reduced.explain(True))
        print("reduced by location:")
        location_reduced.show()
        print()
        print("reduced by hour:")
        hour_reduced.show(24)
        print()

        # repartitioned = location_reduced.coalesce(1)
        # repartitioned.show()

        # create a search tree and broadcast it to the workers
        tree = ReverseGeocoder.create_tree()
        btree = self.spark_session.sparkContext.broadcast(tree)
        broadcasted_tree = btree.value

        # do reverse geocoding to get the 'average' sentiment per state
        get_state_udf = F.udf(
            lambda z: ReverseGeocoder.get_from_tree_by_string(z, broadcasted_tree).record
        )
        state_tweets = location_reduced.withColumn("state", get_state_udf("location"))\
            .groupBy("state")\
            .agg(F.sum("weight").alias("weight"),
                 F.sum("total_sentiment").alias("state_sentiment"),
                 F.sum("total_deviation").alias("state_deviation"))\
            .withColumn("mean_sentiment", F.col("state_sentiment") / F.col("weight"))\
            .withColumn("standard_deviation", F.col("state_deviation") / F.col("weight"))

        # count_2 = state_tweets.count()

        # print("2nd mapreduce: ")
        # print(state_tweets.explain(True))
        print("reduced by state:")
        state_tweets.show(51)

        # repartitioned_2 = state_tweets.coalesce(1)
        # repartitioned_2.show()

        return state_tweets


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
