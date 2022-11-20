from confluent_kafka import Producer
import json
import random
from random import randint
import numpy
from time import sleep

p = Producer({'bootstrap.servers': '147.182.206.35:9092'})
puntos = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))

for _ in range(10):
    sleep(randint(15,30))
    data = {'temperatura': round(numpy.random.normal(50, 5),2),    'humedad':int(numpy.random.normal(50, 5)), 'direccion': random.choice(puntos)}
    print(data)
    p.poll(0)
    p.produce('lab10grupo10', json.dumps(data, indent=2).encode('utf-8'), callback=delivery_report)
p.flush()
