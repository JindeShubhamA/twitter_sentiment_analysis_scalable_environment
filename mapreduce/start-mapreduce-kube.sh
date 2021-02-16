minikube start

folder=mapreduce-kube

kubectl delete -f $folder
kubectl apply -f $folder

echo "> Driver up and running"
echo "> To see the status of the Kubernetes services and pods, use: minikube dashboard"