composefile=mapreduce-compose.yml

# rebuild the mapreduce image to make sure it is up to date
docker-compose -f $composefile build

# push the images in the background, and get rid of output
docker push richardswesterhof/mapreduce_base &> /dev/null &
docker push richardswesterhof/mapreduce &> /dev/null &

folder=mapreduce-kube

rm -rf $folder
mkdir $folder

./tools/kompose -f $composefile convert -o $folder