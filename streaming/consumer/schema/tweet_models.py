from pyspark.sql.types import (
    StructType, StringType, LongType, TimestampType)


tweet_model_schema = (StructType()
                       .add("user_id", StringType())
                       .add("tweet_id", StringType())
                       .add("tweet", StringType())
                       .add("timestamp", TimestampType())
                       .add("location", StringType())
                       )


