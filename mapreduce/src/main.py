import pyspark
import settings
import os


class SparkDriver(object):

    def __init__(self):
        # determine if we are in kube or not
        kube_mode = os.environ.get(settings.kube_mode_check) == "true"
        # create spark session by creating a config first
        self.spark_conf = pyspark.SparkConf()
        self.spark_conf.setAll(settings.spark_settings)

        if not kube_mode:
            self.spark_conf.set("spark.es.nodes.discovery", False)
            self.spark_conf.set("spark.es.nodes.wan.only", True)

        session_builder = pyspark.sql.SparkSession.builder
        session_builder.config(conf = self.spark_conf)
        # create the actual spark session
        self.spark_session = session_builder.getOrCreate()
        # print some helpful information
        print("Configured Spark and Spark Driver")
        print(f"Running PySpark in {'kubernetes' if kube_mode else 'local'} mode (version {self.spark_session.version})")


    def read_es(self):
        # for now the query is just static, but this could be updated in a loop for example
        q = """{
          "query": { 
            "bool": { 
              "must": [
                { "match": { "user_id": "16217679" }}
              ]
            }
          }
        }"""

        print("Getting tweets from Elasticsearch...")

        retrieved_tweets = self.spark_session.read.format("es")\
            .option("es.nodes", settings.es_cluster_settings["es.nodes"])\
            .option("es.port", settings.es_cluster_settings["es.port"])\
            .option("es.query", q)\
            .load("tweets/_doc")

        print(f"Got {retrieved_tweets.count()} tweets")

        return retrieved_tweets


    def process_tweets(self, tweets):
        # do some processing on the tweets, for now we just create a dataframe with the numbers from 0 to the amount of tweets we got
        tweet_numbers = self.spark_session.createDataFrame([{"tweet_num": i} for i in range(tweets.count())])

        return tweet_numbers


    def store_processed_tweets(self, processed_tweets):
        print("Writing to es cluster...")
        # write to elasticsearch on the set ip (node), port, and index (resource)
        processed_tweets.write.format("es")\
            .option("es.nodes", settings.es_cluster_settings["es.nodes"])\
            .option("es.port", settings.es_cluster_settings["es.port"])\
            .option("es.resource", settings.es_resource_names["write_resource"])\
            .save()

        print("Done!")



if __name__ == "__main__":
    spark_driver = SparkDriver()

    r_tweets = spark_driver.read_es()
    p_tweets = spark_driver.process_tweets(r_tweets)
    spark_driver.store_processed_tweets(p_tweets)
