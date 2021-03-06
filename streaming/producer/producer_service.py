
import os
import time
from pathlib import Path
from json import dumps
from confluent_kafka import Producer

from utils import (
    set_logger, config_reader, acked, current_milli_time)
from es.model import esDriver

LOGGER = set_logger("producer_logger")
PARENT_PATH = os.fspath(Path(__file__).parents[1])
CONFIG_PATH = os.path.join(
    PARENT_PATH,
    "configurations",
    "settings.ini")
KAFKA_CONFIG_DICT = config_reader(
    CONFIG_PATH, "kafka")
STREAM_TOPIC = config_reader(
    CONFIG_PATH, "app.settings")["topic_raw"]


def produce_list_of_tweet_dict_into_kafka(list_of_dict):
    producer = Producer(KAFKA_CONFIG_DICT)
    for tweet in list_of_dict:
        tweet = tweet['_source']
        tweet["timestamp_logger"]= current_milli_time()
        print("tweet is ", tweet)
        try:
            producer.produce(
                topic=STREAM_TOPIC,
                value=dumps(tweet).encode("utf-8"),
                callback=acked)
            producer.poll(1)
        except Exception as e:
            LOGGER.info(
                "There is a problem with kafka producer\n"
                "The problem is: {e}!")


if __name__ == "__main__":
    es = esDriver()
    while True:
        es_response = list(es.scanAll())
        produce_list_of_tweet_dict_into_kafka(es_response)
        LOGGER.info("Produced into Kafka topic: {STREAM_TOPIC}.")
        LOGGER.info("Waiting for the next round...")
        time.sleep(10)
