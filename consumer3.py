from kafka import KafkaConsumer
from json import loads
from elasticsearch import Elasticsearch
from time import sleep
consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group3',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

for message in consumer:
    data = loads(str(message.value))
    res = es.index(index='weather',doc_type='current_temperature',id=1,body=data)
    print(res)