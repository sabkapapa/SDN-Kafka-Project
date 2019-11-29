import requests
from time import sleep
from json import dumps
from kafka import KafkaProducer

r = requests.get("https://samples.openweathermap.org/data/2.5/forecast/hourly?id=524901&appid=b6907d289e10d714a6e88b30761fae22")

data = r.text
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


for e in range(1000):
    producer.send('numtest', value=data)
    sleep(5)
