from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class Source:
    def __init__(self):
        self.conf = SparkConf() \
            .setAppName("Spark Stream Service") \
            .setMaster("spark://spark-leader:7077")

        self.sc = SparkContext(conf=self.conf)

        self.spark = SparkSession.builder \
            .config(conf=self.conf) \
            .getOrCreate()
