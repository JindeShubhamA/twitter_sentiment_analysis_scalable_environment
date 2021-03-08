from pyspark.sql.functions import (
    from_json, col, window, avg, desc, max, min)

from spark_base import Source
from schema.tweet_models import tweet_model_schema
from pyspark.sql.types import StructField, StructType, StringType, DoubleType
import json

def connect_to_kafka_stream(topic_name, spark_session):
    return (spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "http://kafka:29092")
            .option("subscribe", topic_name)
            .load())

if __name__ == "__main__":
    s = Source()
    s.sc.setLogLevel("ERROR")

    schema = StructType(
        [
            StructField("user_id", StringType()),
            StructField("tweet_id", DoubleType()),
            StructField("tweet", StringType()),
            StructField("timestamp", StringType()),
            StructField("location", StringType()),
        ]
    )

    df = connect_to_kafka_stream(
        topic_name="stream", spark_session=s.spark)

    df=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    query = (df
             .writeStream
             .option("truncate", "false")
             .outputMode("append")
             .format("console")
             .start()
             )
    query.awaitTermination()
