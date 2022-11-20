from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': '147.182.206.35:9092',
    'group.id': 'grupo10',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['lab10grupo10'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

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