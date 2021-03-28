import os

# the environment variable that will be set to "true" in the kubernetes containers
KUBE_MODE_VAR = "RUNNING_AS_KUBE_DEPLOYMENT"
IN_KUBE_MODE = os.environ.get(KUBE_MODE_VAR) == "true"

# this actually affects the amount of workers we want in local mode
NUM_LOCAL_WORKERS = 4
# whereas this is only to get the correct amount of partitions in kube mode
# and should therefore be set equal to the amount of replicas the worker pod has
NUM_CLUSTER_WORKERS = 2
# controls the amount of partitions there should be PER WORKER
PARTITIONS_PER_WORKER = 4


# general spark settings
spark_settings = [
    ("spark.master", "spark://spark-leader:7077" if IN_KUBE_MODE else f"local[{NUM_LOCAL_WORKERS}]"),
    ("spark.app.name", "Historical Tweet Processor"),
    # client mode should be better if the driver and the workers are on the same network
    # (since they are on the same kubernetes cluster, this seems appropriate)
    ("spark.submit.deployMode", "client"),
    ("spark.ui.showConsoleProgress", "true"),
    # config for the history server
    ("spark.eventLog.enabled", "true"),
    ("spark.eventLog.dir", "file:///var/log"),
    ("spark.history.fs.logDirectory", "file:///var/log"),
    # would print the spark conf if set to true
    ("spark.logConf", "false"),
    # these are important for the spark cluster to communicate back to the driver
    ("spark.driver.bindAddress", "0.0.0.0"),
    ("spark.driver.host", "spark-driver" if IN_KUBE_MODE else "localhost"),
    ("spark.kubernetes.driver.pod.name", "spark-driver"),
    ("spark.driver.port", "30001"),
    ("spark.driver.blockManager.port", "30002"),
    # add this jar to communicate with elasticsearch
    ("spark.jars", "./spark-jars/elasticsearch-hadoop-7.11.1.jar"),
    # control the amount of partitions in the data
    ("spark.sql.shuffle.partitions", f"{PARTITIONS_PER_WORKER * (NUM_CLUSTER_WORKERS if IN_KUBE_MODE else NUM_LOCAL_WORKERS)}"),
]

# settings related to connecting to elasticsearch
es_cluster_settings = {
    "es.nodes" : "elasticsearch" if IN_KUBE_MODE else "localhost",
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