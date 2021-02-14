#!/bin/bash

repo=richardswesterhof

echo Building spark images for python...
$SPARK_HOME/bin/docker-image-tool.sh -r $repo -t latest -p $SPARK_HOME/kubernetes/dockerfiles/spark/bindings/python/Dockerfile build


echo Pushing spark image to $repo/spark...
docker push $repo/spark

echo Pusing python spark image to $repo/spark-py...
docker push $repo/spark-py