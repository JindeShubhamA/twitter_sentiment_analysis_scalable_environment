filename=./kubernetes/spark-kube-config.yml

kubectl delete -f $filename

minikube stop

minikube delete

