# applies all files from the kubernetes subdir
minikube start

kubectl delete -f kubernetes
kubectl apply -f kubernetes