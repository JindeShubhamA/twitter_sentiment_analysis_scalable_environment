from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# check that it really works by running a job
# example from http://spark.apache.org/docs/latest/rdd-programming-guide.html#parallelized-collections
data = range(10000)
distData = sc.parallelize(data)
print(distData.filter(lambda x: not x&1).take(10))
# Out: [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]