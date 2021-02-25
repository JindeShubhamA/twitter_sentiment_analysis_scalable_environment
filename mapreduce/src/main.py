import json
import pyspark
import settings


class SparkDriver(object):

    def __init__(self):
        self.spark_conf = pyspark.SparkConf()
        self.spark_conf.setAll(settings.spark_settings)
        session_builder = pyspark.sql.SparkSession.builder
        session_builder.config(conf = self.spark_conf)
        # add the cluster settings for elasticsearch as well
        # session_builder.config("es.nodes", settings.es_cluster_settings["es.nodes"])
        # session_builder.config("es.port", settings.es_cluster_settings["es.port"])

        self.spark_session = session_builder.getOrCreate()
        print("Configured Spark and Spark Driver")
        print(f"Running PySpark version {self.spark_session.version}")


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

        # es_read_conf = {
        #     **settings.es_cluster_settings,
        #     "es.resource": settings.es_resource_names["read_resource"],
        #     "es.query": q
        # }

        print("Getting tweets from Elasticsearch...")

        # retrieved_tweets = self.spark_session.newAPIHadoopRDD(
        #     inputFormatClass = settings.hadoop_class_settings["inputFormatClass"],
        #     keyClass = settings.hadoop_class_settings["keyClass"],
        #     valueClass = settings.hadoop_class_settings["valueClass"],
        #     conf = es_read_conf
        # )

        retrieved_tweets = self.spark_session.read.format("es")\
            .option("es.nodes", settings.es_cluster_settings["es.nodes"])\
            .option("es.port", settings.es_cluster_settings["es.port"])\
            .load("tweets/_doc")

        print(f"Got {retrieved_tweets.count()} tweets")

        return retrieved_tweets


    def process_tweets(self, tweets):
        tweet_numbers = self.spark_session.createDataFrame([{"tweet_num": i} for i in range(tweets.count())])
        # tweet_nums_json = tweet_numbers.map(lambda x: x.pop("_id", "")).map(json.dumps).map(lambda x: ("key", x))

        return tweet_numbers


    def store_processed_tweets(self, processed_tweets):
        print("Writing to es cluster...")
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
