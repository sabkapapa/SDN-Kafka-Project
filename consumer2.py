from kafka import KafkaConsumer
#from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group2',
     value_deserializer=lambda x: loads(x.decode('utf-8')))


from flask import Flask
app = Flask(__name__)


@app.route('/')
def hello():
    received_data = "("
    i = 0
    for message in consumer:
        message = message.value
        print(message)
        if "list" not in message:
            continue
        st_temp = loads(message)
        print(st_temp["list"])
        received_data += "Temperature: at Bangalore  " + str(st_temp["list"][0]["main"]["temp"])
        i += 1
        break
    received_data += ')'
    return received_data

if __name__ == '__main__':
    app.run()
