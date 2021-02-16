# rebuild the mapreduce image to make sure it is up to date
docker-compose build
# push it so other people have the most up to date image as well
docker push richardswesterhof/mapreduce

filename=./kubernetes/

rm -rf kubernetes
mkdir kubernetes

./tools/kompose convert -o $filename