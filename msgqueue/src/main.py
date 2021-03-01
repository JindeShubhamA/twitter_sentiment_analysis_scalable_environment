import pyspark
import settings
# import os
from kafka import KafkaConsumer
from json import loads
from time import sleep
#
class SparkDriver(object):
    def __init__(self):
        f = open("demofile3.txt", "a+")
        f.write("--")
        f.write('1')
        f.write("\n")
        # determine if we are in kubernetes or not
        # kube_mode = os.environ.get(settings.kube_mode_check) == "true"
        f.write('2')
        f.write("\n")
        # create spark session by creating a config first
        self.spark_conf = pyspark.SparkConf()
        f.write('3')
        f.write("\n")
        self.spark_conf.setAll(settings.spark_settings)
        f.write('4')
        f.write("\n")
        # if we are not inside kubernetes, that means we likely won't have access to the elasticsearch nodes
        # so we change some settings that allow us to communicate with elasticsearch across different networks
        # if not kube_mode:
        #     self.spark_conf.set("spark.es.nodes.discovery", False)
        #     self.spark_conf.set("spark.es.nodes.wan.only", True)
        f.write('5')
        f.write("\n")
        session_builder = pyspark.sql.SparkSession.builder
        f.write('6')
        f.write("\n")
        session_builder.config(conf = self.spark_conf)
        f.write('7')
        f.write("\n")
        # create the actual spark session
        self.spark_session = session_builder.getOrCreate()
        f.write('8')
        f.write("\n")
        # f.close()
        # print some helpful information
        # print("Configured Spark and Spark driver")
        # print(f"Spark driver is in {'kubernetes' if kube_mode else 'local'} mode")
        # print(f"Running PySpark version {self.spark_session.version}")


    def consume_tweets(self):
        # # for now the query is just static, but this could be updated in a loop for example
        # q = """{
        #   "query": {
        #     "bool": {
        #       "must": [
        #         { "match": { "user_id": "16217679" }}
        #       ]
        #     }
        #   }
        # }"""
        #
        # print("Getting tweets from Elasticsearch...")
        #
        # retrieved_tweets = self.spark_session.read.format("es")\
        #     .option("es.nodes", settings.es_cluster_settings["es.nodes"])\
        #     .option("es.port", settings.es_cluster_settings["es.port"])\
        #     .option("es.query", q)\
        #     .load("tweets/_doc")
        #
        # print(f"Got {retrieved_tweets.count()} tweets")
        #
        # return retrieved_tweets
        f = open("demofile3.txt", "a+")
        f.write("--")
        f.write('Starting Consumer')
        f.write("\n")
        print("Starting Consumer")
        consumer = KafkaConsumer(
            'topic_test1',
            bootstrap_servers=['kafka:9093'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group-id',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
        f.write("--")
        f.write('Reading events')
        f.write("\n")
        for event in consumer:
            event_data = event.value
            # Do whatever you want
            print(event_data)


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


#
# if __name__ == "__main__":
#     f = open("demofile3.txt", "w+")
#     f.write("--")
#     f.write('Spark Driver Creating')
#     f.write("\n")
#     # f = open("demofile3.txt", "a+")
#     f.close()
spark_driver = SparkDriver()
#     f = open("demofile3.txt", "w+")
#     f.write("--")
#     f.write('Spark Driver Created')
#     # f.close()
#     # r_tweets = spark_driver.consume_tweets()
#     # p_tweets = spark_driver.process_tweets(r_tweets)
#     # spark_driver.store_processed_tweets(p_tweets)
#     f.write("--")
#     f.write('Starting Consumer')
#     f.write("\n")
spark_driver.consume_tweets()
# consumer = KafkaConsumer(
#     'topic_test1',
#     bootstrap_servers=['kafka:9093'],
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='my-group-id',
#     value_deserializer=lambda x: loads(x.decode('utf-8'))
# )
# # f.write("--")
# # f.write('Reading events')
# # f.write("\n")
# for event in consumer:
#     event_data = event.value
#     # Do whatever you want
#     print(event_data)

