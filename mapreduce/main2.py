from pyspark import SparkConf
from pyspark.sql import SparkSession

# local[2] enables dual threaded execution
conf = SparkConf().setMaster("local[2]").setAppName("MapReduce")

spark = SparkSession \
    .builder \
    .config(conf) \
    .getOrCreate()


def do_stuff():
    df = spark.read.load("../dataset/tweet_data.csv", format="csv", sep='\t', inferSchema="true", header="true")
    type(df)


if __name__ == "__main__":
    do_stuff()