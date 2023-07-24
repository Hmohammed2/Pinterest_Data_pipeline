from kafka import KafkaConsumer
from json import loads

cluster_consumer = KafkaConsumer(
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: loads(message),
    auto_offset_reset = "earliest"
    )

cluster_consumer.subscribe(topics=["topic1"])

for message in cluster_consumer:
    print(message.value)