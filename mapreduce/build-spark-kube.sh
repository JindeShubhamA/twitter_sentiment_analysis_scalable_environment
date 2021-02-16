composefile=spark-compose.yml

# rebuild the spark image to make sure it is up to date
docker-compose -f $composefile build

folder=spark-kube

rm -rf $folder
mkdir $folder

./tools/kompose -f $composefile convert -o $folder