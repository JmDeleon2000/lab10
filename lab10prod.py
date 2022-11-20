from confluent_kafka import Producer
import json
from faker import Faker
import random
from random import randint

p = Producer({'bootstrap.servers': '147.182.206.35:9092'})

puntos = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


# for data in some_data_source:
for _ in range(10):
    data = {'temperatura': round(random.uniform(0, 100),2),    'humedad': randint(
        0, 100), 'direccion': random.choice(puntos)}
    print(data)
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    p.produce('lab10grupo10', json.dumps(data, indent=2).encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
