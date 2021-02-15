docker-compose build
docker push richardswesterhof/mapreduce


rm ./kubernetes
./tools/kompose convert -o kubernetes