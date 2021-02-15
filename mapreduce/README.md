# About
Our mapreduce program uses pyspark for the computations. 
The Spark cluster is set up to run each worker (and the master as well) in a separate container, which can communicate over a network ``spark-communication``.
Any driver program that submits jobs to the Spark cluster should join this network to gain access to the cluster's API.

It is available in docker-compose and kubernetes, the provided commands use kubernetes (docker-compose should be as simple as running ``docker-compose up --build``)


# How to run (on linux)
Make sure you have docker, docker-compose, and minikube installed.

Then simply run these commands in the ``mapreduce`` directory (should be where this readme is too).
This will pull and build all necessary images, start minikube, and launch the cluster onto kubernetes according to ``docker-compose.yml``
```
./build-spark-kube.sh
```
```
./start-spark-kube.sh
```

To stop and delete everything, run
```
./stop-spark-kube.sh
```