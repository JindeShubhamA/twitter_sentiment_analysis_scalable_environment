import os

# the environment variable that will be set to "true" in the kubernetes containers
kube_mode_check = "RUNNING_AS_KUBE_DEPLOYMENT"

# general spark settings
spark_settings = [
    ("spark.master", "spark://spark-leader:7077" if os.environ.get(kube_mode_check) == "true" else "local[4]"),
    ("spark.app.name", "Historical Tweet Processor"),
    # client mode should be better if the driver and the workers are on the same network
    # (since they are on the same docker network, this seems appropriate)
    ("spark.submit.deployMode", "client"),
    ("spark.ui.showConsoleProgress", "true"),
    ("spark.eventLog.enabled", "false"),
    ("spark.logConf", "false"),
    # these are important for spark to communicate back to us
    ("spark.driver.bindAddress", "0.0.0.0"),
    ("spark.driver.host", "spark-driver" if os.environ.get(kube_mode_check) == "true" else "localhost"),
    ("spark.kubernetes.driver.pod.name", "spark-driver"),
    ("spark.driver.port", "30001"),
    ("spark.driver.blockManager.port", "30002"),
    # add this jar to communicate with elasticsearch
    ("spark.jars", "./spark-jars/elasticsearch-hadoop-7.11.1.jar"),
]

# settings related to connecting to elasticsearch
es_cluster_settings = {
    "es.nodes" : "elasticsearch" if os.environ.get(kube_mode_check) == "true" else "localhost",
    "es.port" : "9200",
    "es.read.metadata": "true",
    "es.write.operation": "upsert",
    "es.mapping.id": "id",
    "mode": "append"
}

# settings related to the resources in elasticsearch
es_resource_names = {
    "read_resource": "tweets",
    "write_resource": "tweet_numbers"
}

# hadoop settings (these are currently unused, were used for RDD stuff)
hadoop_class_settings = {
    "inputFormatClass": "org.elasticsearch.hadoop.mr.EsInputFormat",
    "outputFormatClass": "org.elasticsearch.hadoop.mr.EsOutputFormat",
    "keyClass": "org.apache.hadoop.io.NullWritable",
    "valueClass": "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    "path": "-"
}