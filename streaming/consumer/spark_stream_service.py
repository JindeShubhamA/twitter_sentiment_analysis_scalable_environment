from pyspark.sql.functions import (
    from_json, col, window, avg, desc, max, min, udf)

# import numpy as np
from spark_base import Source
from schema.tweet_models import tweet_model_schema
from pyspark.sql.types import StructField, StructType, StringType, DoubleType
import json

import re
import textblob
from textblob import TextBlob
from pyspark.sql import functions as F


import shapefile
from typing import Optional as Opt, Tuple

from shapefile import Shapes
from shapely.geometry import shape, Point
from random import randrange





def find_by_string(coord_string) :
    coords = coord_string.split(",")
    return  float(coords[0]), float(coords[1])


def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w+:\ / \ / \S+)", " ", tweet).split())


def get_tweet_sentiment(tweet):
    analysis = TextBlob(clean_tweet(tweet))
    return analysis.sentiment.polarity


def get_location(lat_lon):
    # shp = open("scripts/consumer/shapefiles/us_states.shp", "rb")
    # dbf = open("scripts/consumer/shapefiles/us_states.dbf", "rb")
    # shp_reader = shapefile.Reader(shp=shp, dbf=dbf)
    # all_shapes = shp_reader.shapes()  # get all the polygons
    # all_records = shp_reader.records()
    # j = 0
    #
    # for i in range(len(all_shapes)):
    #     boundary = all_shapes[i]  # get a boundary polygon
    #     shape_bndry = shape(boundary)
    #     point = Point(find_by_string(lat_lon))
    #     if point.within(shape_bndry):
    #         j = 1
    return "CALIFORNIA"
    # if not j:
    #     k = randrange(49)
    #     return all_records[k][3]
    # else:
    #     return all_records[j][3]



def connect_to_kafka_stream(topic_name, spark_session):
    return (spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "http://kafka:9093")
            .option("subscribe", topic_name)
            .load())


if __name__ == "__main__":
    s = Source()
    s.sc.setLogLevel("ERROR")

    schema = StructType(
        [
            StructField("user_id", StringType()),
            StructField("tweet_id", StringType()),
            StructField("tweet", StringType()),
            StructField("timestamp", StringType()),
            StructField("location", StringType()),
        ]
    )

    df = connect_to_kafka_stream(
        topic_name="stream", spark_session=s.spark)

    get_state_udf = udf(lambda z: get_location(z), StringType())

    sentiment_udf = udf(lambda z: get_tweet_sentiment(z), StringType())

    #df =df.selectExpr("CAST(value AS STRING) AS json").select(from_json(col("json"), schema).alias("data")).select("data.tweet",sentiment_udf('data.tweet').alias('sentiment'))

    df = df.selectExpr("CAST(value AS STRING) AS json").select(from_json(col("json"), schema).alias("data")).select("data.location",get_state_udf('data.location').alias('state'),"data.tweet",sentiment_udf('data.tweet').alias('sentiment'))



    query = (df
             .writeStream
             .option("truncate", "false")
             .outputMode("append")
             .format("console")
             .start()
             )
    # query1.awaitTermination()
    query.awaitTermination()
