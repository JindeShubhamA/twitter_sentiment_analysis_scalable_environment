minikube start

filename=./kubernetes/

kubectl delete -f $filename
kubectl apply -f $filename

echo "> Spark cluster up and running, to tunnel into Spark Master UI, use: minikube service spark-leader"
echo "> To see the status of the Kubernetes services and pods, use: minikube dashboard"