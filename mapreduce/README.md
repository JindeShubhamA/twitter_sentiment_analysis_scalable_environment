# About
Our mapreduce program uses PySpark for the computations. 
The Spark cluster is set up to run each worker (and the master as well) in a separate container.

It is available in docker-compose and Kubernetes.

# Prerequisites
First, make sure you have docker, docker-compose, and minikube installed. 
If you want to run the driver outside a docker container, also make sure you have Spark 2.4.6, Java 8, and the pip requirements installed.
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