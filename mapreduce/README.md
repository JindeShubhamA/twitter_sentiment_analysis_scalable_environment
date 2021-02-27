# About
Our mapreduce program uses pyspark for the computations. 
The Spark cluster is set up to run each worker (and the master as well) in a separate container, which can communicate over a network ``spark-communication``.
Any driver program that submits jobs to the Spark cluster should join this network to gain access to the cluster's API.

It is available in docker-compose and kubernetes.

# Prerequisites
First, make sure you have docker, docker-compose, and minikube installed. 
Follow the steps from this README inside the ``mapreduce`` directory (unless otherwise specified).

# How to run

## Running the Spark cluster
In kubernetes:
```
kubectl apply -f spark-kube
```

In docker-compose:
```
docker-compose -f spark-compose.yml up
```

## Running the Spark driver
In kubernetes:
```
kubectl apply -f mapreduce-kube
```

In docker-compose:
```
docker-compose -f mapreduce-compose.yml up
```

Locally (i.e. without a Spark cluster):
```
python3 ./src/main.py
```
In this case you have to have Spark 2.4.6 installed locally, together with Python 3.6 (higher may be incompatible).
Elasticsearch needs to be reachable on localhost:9200 (by running it locally or in Kubernetes and port forward 9200).


(In the current state, the driver simply submits one job and exits, 
which causes it to be respawned by Kubernetes, submit one job, exit, et cetera ad infinitum)