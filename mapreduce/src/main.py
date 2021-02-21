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

# spark = SparkSession.builder.config(spark_conf).getOrCreate()

print("configured spark")


# q = """{
#     "query": {
#         "bool" : {
#             "must" : [
#                 "match_all" : {}
#             ],
#            "filter" : {
#                 "user_id": "16217679"
#             }
#         }
#     }
# }"""

q = """{
  "query": { 
    "bool": { 
      "must": [
        { "match": { "user_id": "16217679" }}
      ]
    }
  }
}"""

# NUM_SAMPLES = 10000
#
#
# def inside(p):
#     x, y = random(), random()
#     return x * x + y * y < 1
#
#
# count = sc.parallelize(range(0, NUM_SAMPLES)) \
#     .filter(inside).count()
# print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))

es_read_conf = {
    "es.nodes" : "elasticsearch",
    "es.port" : "9200",
    "es.resource" : "tweets/_doc",
    "es.query" : q
}

es_rdd = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_read_conf)


print("output below:")
print(es_rdd)

# print(tweets)