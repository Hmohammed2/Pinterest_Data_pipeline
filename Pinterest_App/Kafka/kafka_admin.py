from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id = "Kafka-Administrator"
)

topics = []
topics.append(NewTopic(name="ML_data", num_partitions=3, replication_factor=1))
topics.append(NewTopic(name="Retail_data", num_partitions=2, replication_factor=1))

print(admin_client.create_topics(new_topics=topics))

