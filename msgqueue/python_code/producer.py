import json
from time import sleep
from json import dumps
from es import model
from es.model import esDriver
from kafka import KafkaProducer
# print("here")
es = esDriver()
#
es.create()
# #
es.populate('tweet_data_500k.csv')
#
es_response = list(es.scanAll())
print(es_response)
# #
#es_response = list(es_response['hits']['hits'])

producer = KafkaProducer(
    bootstrap_servers=['kafka:9093'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
#

for item in es_response:
    print(json.dumps(item))
    data = {'counter': json.dumps(item)}
    # Adding file write for debugging. Will be removed afterwards
    f = open("demofile3.txt", "a+")
    f.write("--")
    f.write(str(data))
    f.write("\n")
    producer.send('topic_test', value=data)
    sleep(0.5)

# for j in range(9999):
#     print("Iteration", j)
#     data = {'counter': j}
#     producer.send('topic_test', value=data)
#     sleep(0.5)