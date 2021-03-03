import pyspark
from pyspark.sql import DataFrame

import settings
import os
from reverse_geocoder import ReverseGeocoder
from tweet_processing import *

import shutil


def clean_tweet(tweet: str) -> str:
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w+:\ / \ / \S+)", " ", tweet).split())


def get_tweet_sentiment(tweet: str) -> float:
    analysis = TextBlob(clean_tweet(tweet))
    # use this to get the raw sentiment
    # return analysis.sentiment.polarity

    # use this to get whether the tweet is positive or not
    if analysis.sentiment.polarity > 0:
        return 1
    else:
        return 0


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

        # add the shapefiles to the spark context so the workers have access to them as well
        # self.spark_session.sparkContext.addFile(os.path.abspath("./shapefiles/"), recursive=True)
        # add the files with the custom classes so the workers have access to them as well
        self.spark_session.sparkContext.addPyFile("reverse_geocoder.py")
        self.spark_session.sparkContext.addPyFile("geocoding_result.py")
        self.spark_session.sparkContext.addPyFile("search_tree.py")

        # package_location = "/usr/local/lib/python3.6/site-packages" if kube_mode else "../venv/Lib/site-packages"
        # zip_location = "./zipped-packages"
        # # add any package names that need to be present on the worker nodes to this list
        # # keep in mind that any other packages these packages depend on need to added as well!
        # # (use "pip show package_name" to see which packages a package requires)
        # worker_packages = ["nltk", "click", "joblib", "regex", "tqdm", "textblob"]
        # # zip the packages from worker_packages, and add them to the spark_context so the workers can use them
        # for package in worker_packages:
        #     zipped_package_location = os.path.abspath(
        #         shutil.make_archive(
        #             os.path.join(zip_location, package),
        #             "zip",
        #             root_dir=package_location,
        #             base_dir=package
        #         )
        #     )
        #     self.spark_session.sparkContext.addPyFile(zipped_package_location)

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
        # do some processing on the tweets, for now we just create a dataframe with the numbers from 0 to the amount of tweets we got
        # tweet_numbers = self.spark_session.createDataFrame([{"tweet_num": i, "id": i} for i in range(tweets.count())])

        # do sentiment analysis
        # reduces each tweet to a tuple (location, nr_positive_tweets)
        reduced_tweets = tweets.rdd.map(lambda x: (x.location, get_tweet_sentiment(x.tweet)))\
                                    .reduceByKey(lambda x, y: x + y)
        print("1st mapreduce: ", reduced_tweets.take(10))

        # create a search tree and broadcast it to the workers
        tree = ReverseGeocoder.create_tree()
        btree = self.spark_session.sparkContext.broadcast(tree)
        broadcasted_tree = btree.value

        # do reverse geocoding to get the 'average' sentiment per state
        state_tweets = reduced_tweets.map(lambda x: (ReverseGeocoder.get_from_tree_by_string(x[0], broadcasted_tree).record, x[1]))\
            .reduceByKey(lambda x, y: x + y).collect()
        print(f"2nd mapreduce ({len(state_tweets)} items):", state_tweets)

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
