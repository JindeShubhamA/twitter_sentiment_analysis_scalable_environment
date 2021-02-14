#!/bin/bash



$SPARK_HOME/bin/spark-submit --master k8s://https://127.0.0.1:49169 --conf spark.kubernetes.container.image=docker.io/richardswesterhof/spark:latest --class org.apache.spark.examples.SparkPi local:///$SPARK_HOME/examples/jars/spark-examples_2.12-3.0.1.jar