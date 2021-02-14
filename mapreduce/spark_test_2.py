from pyspark.sql import SparkSession
import time


time.sleep(10)


logFile = "spark_test.py"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").master("spark://localhost:7077").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()