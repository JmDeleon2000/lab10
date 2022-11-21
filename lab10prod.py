from confluent_kafka import Producer
import json
import random
from random import randint
import numpy
from time import sleep
from bitpacker import pack



direcciones = {'N':0, 'NW':1, 'W':2, 'SW':3, 'S':4, 'SE':5, 'E':6, 'NE':7}
p = Producer({'bootstrap.servers': '147.182.206.35:9092'})
puntos = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))

for _ in range(10):
    # sleep(randint(15,30))
    
    ## normal
    # data = {'temperatura': round(numpy.random.normal(50, 5),2),    'humedad':int(numpy.random.normal(50, 5)), 'direccion': random.choice(puntos)}
    # print(data)
    # p.poll(0)
    # p.produce('lab10grupo10', json.dumps(data, indent=2).encode('utf-8'), callback=delivery_report)

    ## packed
    temperatura = round(numpy.random.normal(50, 5),2)
    humedad = int(numpy.random.normal(50, 5))
    direccion = random.choice(range(len(puntos)))
    data = {'temperatura': temperatura,    'humedad': humedad, 'direccion': direccion}
    print(data)
    datapak = pack(temperatura, direccion, humedad)
    print(datapak)
    p.poll(0)
    p.produce('lab10grupo10', datapak, callback=delivery_report)

p.flush()
