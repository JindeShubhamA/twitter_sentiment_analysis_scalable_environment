# Spark Structured Streaming Project Real time tweets analysis

A Python project for analysing real-time tweets data

To build the project

`docker-compose build`

To run the project:

`docker-compose up -d zookeeper elasticsearch kafka spark-leader spark-worker-1`

After this run client to see elasticsearch is up or not

`docker-compose up client`

Populate the elasticsearch

`docker-compose up -d populate_db`

Run the spark streaming consumer

`docker exec spark-leader bash scripts/start_consumer.sh`

Run the producer in a seperate tab

`docker-compose up producer`
## Producer

Producer will query the tweets from the elasticsearch and spark streaming will consume the tweets.

Spark processing of the tweets is still not applied


