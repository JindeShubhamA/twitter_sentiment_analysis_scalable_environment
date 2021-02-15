from random import random

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
    ('spark.driver.bindAddress', '0.0.0.0'),
    ('spark.driver.host', 'spark-driver'),
    ('spark.kubernetes.driver.pod.name', 'spark-driver'),
    ('spark.driver.port', '30001'),
    ('spark.driver.blockManager.port', '30002'),
])

sc = pyspark.SparkContext(conf=spark_conf).getOrCreate()
NUM_SAMPLES = 10000


def inside(p):
    x, y = random(), random()
    return x * x + y * y < 1


count = sc.parallelize(range(0, NUM_SAMPLES)) \
    .filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))