import json
import time
from random import random
from pyspark.sql import SparkSession
import pyspark

spark_conf = pyspark.SparkConf()
spark_conf.setAll([
    ('spark.master', "spark://spark-leader:7077"),
    ('spark.app.name', 'TestingSpark'),
    # client mode should be better if the driver and the workers are on the same network
    # (since they are on the same docker network, this seems appropriate)
    ('spark.submit.deployMode', 'client'),
    ('spark.ui.showConsoleProgress', 'true'),
    ('spark.eventLog.enabled', 'false'),
    ('spark.logConf', 'false'),
    # these are important for spark to communicate back to us
    ('spark.driver.bindAddress', '0.0.0.0'),
    ('spark.driver.host', 'spark-driver'),
    ('spark.kubernetes.driver.pod.name', 'spark-driver'),
    ('spark.driver.port', '30001'),
    ('spark.driver.blockManager.port', '30002'),
    # add this to communicate with elastic
    ('spark.jars', './spark-jars/elasticsearch-hadoop-7.11.1.jar'),
])

sc = pyspark.SparkContext(conf=spark_conf).getOrCreate()

print("configured spark")

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
    "es.nodes" : "elasticsearch",
    "es.port" : "9200",
    "es.resource" : "tweets/_doc",
    "es.query" : q
}

retrieved_tweets = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_read_conf)


print(f"got ${retrieved_tweets.count()} tweets")

es_write_conf = {
    "es.nodes" : 'elasticsearch',
    "es.port" : '9200',
    "es.resource" : '%s/%s' % ('tweet_numbers', 'tweet_num'),
    "es.input.json": 'true'
}

tweet_numbers = sc.parallelize([{'tweet_num': i} for i in range(retrieved_tweets.count())])

def remove__id(doc):
    # `_id` field needs to be removed from the document
    # to be indexed, else configure this in `conf` while
    # calling the `saveAsNewAPIHadoopFile` API
    doc.pop('_id', '')
    return doc


tweet_nums_rdd = tweet_numbers.map(remove__id).map(json.dumps).map(lambda x: ('key', x))

print("Writing to es cluster...")

tweet_nums_rdd.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_write_conf
)

print("Done!")