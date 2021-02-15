# About
Our mapreduce program uses pyspark for the computations. 
The Spark cluster is set up to run each worker (and the master as well) in a separate container, which can communicate over a network ``spark-communication``.
Any driver program that submits jobs to the Spark cluster should join this network to gain access to the cluster's API.


# how to run
simply run this command in the ``mapreduce`` directory (should be where this readme is too).
This will pull and build all necessary images
```
docker-compose up --build
```

open ``localhost:8080``

wait some time

refresh page

???

Profit