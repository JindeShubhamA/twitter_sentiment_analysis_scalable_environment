import pyspark
from pyspark.sql import DataFrame


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
        #   "from" : 0, "size" : 10000,
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

        # do sentiment analysis
        # reduces data frame to 2 cols: location | sentiment_score_sum
        reduced_tweets = tweets.withColumn("tweet_sentiment", get_sentiment_udf("tweet"))\
            .groupBy("location")\
            .agg(F.sum("tweet_sentiment").alias("tweet_sentiment"))

        # count = reduced_tweets.count()

        print("1st mapreduce: ")
        # print(f"{count} rows")
        # print(reduced_tweets.explain(True))
        reduced_tweets.show()

        # repartitioned = reduced_tweets.coalesce(1)
        # repartitioned.show()

        # create a search tree and broadcast it to the workers
        tree = ReverseGeocoder.create_tree()
        btree = self.spark_session.sparkContext.broadcast(tree)
        broadcasted_tree = btree.value

        # do reverse geocoding to get the 'average' sentiment per state
        get_state_udf = F.udf(
            lambda z: ReverseGeocoder.get_from_tree_by_string(z, broadcasted_tree).record
        )
        state_tweets = reduced_tweets.withColumn("state", get_state_udf("location"))\
            .groupBy("state")\
            .agg(F.sum("tweet_sentiment").alias("state_sentiment"))

        # count_2 = state_tweets.count()

        print("2nd mapreduce: ")
        # print(state_tweets.explain(True))
        state_tweets.show()

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
