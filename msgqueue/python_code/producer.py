import json
from time import sleep
from json import dumps
from es import model
from es.model import esDriver
from kafka import KafkaProducer
print("here")
# es = esDriver()
# #
# # # es.create()
# # #
# # # es.populate('tweet_data_500k.csv')
# #
# es_response = es.scanAll()
# print(es_response)
#

#
# for i in range(1,100):
#     print(i)
#     producer.send('topic_test', value=i)
#
#
# # for item in es_response:
# #     print(json.dumps(item))
# #     producer.send('topic_test', value=json.dumps(item))
# #     sleep(0.5)

producer = KafkaProducer(
    bootstrap_servers=['kafka:9093'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
for j in range(9999):
    print("Iteration", j)
    data = {'counter': j}
    producer.send('topic_test', value=data)
    sleep(0.5)