#!/bin/bash

spark/bin/spark-submit \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.2 \
	consumer/spark_stream_service.py
