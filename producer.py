import requests
from time import sleep
from json import dumps
from kafka import KafkaProducer

r = requests.get("https://samples.openweathermap.org/data/2.5/forecast/hourly?id=524901&appid=b6907d289e10d714a6e88b30761fae22
samples.openweathermap.org
{"cod":"200","message":0.0204,"cnt":96,"list":[{"dt":1553709600,"main":{"temp":272.09,"temp_min":271.358,"temp_max":272.09,"pressure":1018.01,"sea_level":1018.01 ...
samples.openweathermap.org
")

data = r.text
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


for e in range(1000):
    producer.send('numtest', value=data)
    sleep(5)
