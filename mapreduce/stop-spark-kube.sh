filename=./kubernetes/

kubectl delete -f $filename

minikube stop

# you can delete the minkikube cluster if you want,
# but this means it will have to redownload all the images when you restart it
# minikube delete