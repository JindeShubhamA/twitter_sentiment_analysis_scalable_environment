import json
import pyspark
import settings


class SparkDriver(object):

    def __init__(self):
        self.spark_conf = pyspark.SparkConf()
        self.spark_conf.setAll(settings.spark_settings)
        self.spark_context = pyspark.SparkContext(conf = self.spark_conf).getOrCreate()
        print("Configured Spark and Spark Driver")


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

        es_read_conf = {
            **settings.es_cluster_settings,
            "es.resource": settings.es_resource_names.read_resource,
            "es.query": q
        }

        print("Getting tweets from Elasticsearch...")

        retrieved_tweets = self.spark_context.newAPIHadoopRDD(
            inputFormatClass = settings.hadoop_class_settings.inputFormatClass,
            keyClass = settings.hadoop_class_settings.keyClass,
            valueClass = settings.hadoop_class_settings.valueClass,
            conf = es_read_conf
        )

        print(f"Got {retrieved_tweets.count()} tweets")

        return retrieved_tweets


    def process_tweets(self, tweets):
        tweet_numbers = self.spark_context.parallelize([{"tweet_num": i} for i in range(tweets.count())])
        tweet_nums_json = tweet_numbers.map(lambda x: x.pop("_id", "")).map(json.dumps).map(lambda x: ("key", x))

        return tweet_nums_json


    def store_processed_tweets(self, processed_tweets):
        es_write_conf = {
            **settings.es_cluster_settings,
            "es.resource": settings.es_resource_names.write_resource,
            "es.input.json": "true"
        }

        print("Writing to es cluster...")
        processed_tweets.saveAsNewAPIHadoopFile(
            path = settings.hadoop_class_settings.path,
            outputFormatClass = settings.hadoop_class_settings.inputFormatClass,
            keyClass = settings.hadoop_class_settings.keyClass,
            valueClass = settings.hadoop_class_settings.valueClass,
            conf = es_write_conf
        )

        print("Done!")



if __name__ == "__main__":
    spark_driver = SparkDriver()

    r_tweets = spark_driver.read_es()
    p_tweets = spark_driver.process_tweets(r_tweets)
    spark_driver.store_processed_tweets(p_tweets)
