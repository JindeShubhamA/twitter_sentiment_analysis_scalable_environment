spark_settings = [
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
]

es_cluster_settings = {
    "es.nodes" : "elasticsearch",
    "es.port" : "9200"
}

es_resource_names = {
    "read_resource": "tweets/_doc",
    "write_resource": ""
}

hadoop_class_settings = {
    "inputFormatClass": "org.elasticsearch.hadoop.mr.EsInputFormat",
    "outputFormatClass": "org.elasticsearch.hadoop.mr.EsOutputFormat",
    "keyClass": "org.apache.hadoop.io.NullWritable",
    "valueClass": "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    "path": "-"
}