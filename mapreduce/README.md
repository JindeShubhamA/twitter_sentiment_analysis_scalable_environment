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
If you've made any changes to the cluster configuration, run:
```
./build-spark-kube.sh
```
Either way, run this next:
```
./start-spark-kube.sh
```

This will (build and) pull all necessary images for the Spark master and worker, start minikube, 
and launch the cluster onto Kubernetes according to ``spark-compose.yml``

## Running the Spark driver
If you've made changes to the driver configuration or code, run:
```
./build-mapreduce-kube.sh
```
Either way, run this next:
```
./start-mapreduce-kube.sh
```

This will (build and) pull all necessary images for the Spark driver, start minikube, 
and launch the cluster onto Kubernetes according to ``mapreduce-compose.yml``

(In the current state, the driver simply submits one job and exits, 
which causes it to be respawned by Kubernetes, submit one job, exit, et cetera ad infinitum)