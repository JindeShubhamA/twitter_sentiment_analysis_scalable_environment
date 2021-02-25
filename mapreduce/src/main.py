import json
import pyspark
import settings

spark_conf = pyspark.SparkConf()
spark_conf.setAll(settings.spark_settings)

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
    **settings.es_cluster_settings,
    "es.resource" : settings.es_resource_names.read_resource,
    "es.query" : q
}

retrieved_tweets = sc.newAPIHadoopRDD(
    inputFormatClass=settings.hadoop_class_settings.inputFormatClass,
    keyClass=settings.hadoop_class_settings.keyClass,
    valueClass=settings.hadoop_class_settings.valueClass,
    conf=es_read_conf
)


print(f"got {retrieved_tweets.count()} tweets")

es_write_conf = {
    **settings.es_cluster_settings,
    "es.resource" : settings.es_resource_names.write_resource,
    "es.input.json": "true"
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
    path=settings.hadoop_class_settings.path,
    outputFormatClass=settings.hadoop_class_settings.inputFormatClass,
    keyClass=settings.hadoop_class_settings.keyClass,
    valueClass=settings.hadoop_class_settings.valueClass,
    conf=es_write_conf
)

print("Done!")