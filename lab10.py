from confluent_kafka import Consumer
from bitpacker import unpack
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import time

fig, axs = plt.subplots(nrows=2, sharex=True, figsize=(20, 20))

captured_data = []
arriveTimes = []

def animate(i):
    x=list(range(i))

    axs[0].clear()
    axs[0].set_ylabel('Temperatura')
    axs[0].plot(arriveTimes, [y[0] for y in captured_data])
    axs[1].clear()
    axs[1].set_ylabel('Humedad')
    axs[1].plot(arriveTimes, [y[2] for y in captured_data])
    axs[1].set_xlabel('Tiempo')



c = Consumer({
    'bootstrap.servers': '147.182.206.35:9092',
    'group.id': 'grupo10',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['lab10grupo10'])

puntos = ['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE']
ani  = animation.FuncAnimation(fig, animate,  interval=1000)
plt.show()
last = time.time()
while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    now = time.time()
    ## normal
    # print('Received message: {}'.format(msg.value().decode('utf-8')))

    ## packed
    dataunpak = msg.value()
    unpacked = unpack(dataunpak)
    print(f'Temperatura: {unpacked[0]}')
    print(f'Humedad: {unpacked[2]}')
    print(f'Dirección: {puntos[unpacked[1]]}')

    captured_data.append(unpacked)
    arriveTimes.append(now-last)
    last = time.time()
    


c.close()

# from confluent_kafka.admin import AdminClient, NewTopic

# a = AdminClient({'bootstrap.servers': '147.182.206.35:9092'})

# new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in ["lab10grupo10", "grupo10lab10"]]
# # Note: In a multi-cluster production scenario, it is more typical to use a replication_factor of 3 for durability.

# # Call create_topics to asynchronously create topics. A dict
# # of <topic,future> is returned.
# fs = a.create_topics(new_topics)

# # Wait for each operation to finish.
# for topic, f in fs.items():
#     try:
#         f.result()  # The result itself is None
#         print("Topic {} created".format(topic))
#     except Exception as e:
#         print("Failed to create topic {}: {}".format(topic, e))