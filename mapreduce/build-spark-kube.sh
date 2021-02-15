# rebuild the mapreduce image to make sure it is up to date
docker-compose build
# push it so other people have the most up to date image as well
docker push richardswesterhof/mapreduce

filename=./kubernetes/spark-kube-config.yml

mkdir kubernetes

rm $filename
./tools/kompose convert -o $filename