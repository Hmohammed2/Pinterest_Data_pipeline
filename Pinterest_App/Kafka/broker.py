from kafka import KafkaClient
from kafka.cluster import ClusterMetadata

# Create a connection to view metadata
meta_cluster_conn = ClusterMetadata(
    bootstrap_servers="localhost:9092" # Specific broker to assign address to connect to
)

# retrieve metadata about the cluster
print(meta_cluster_conn.brokers())

# Create a connection to the kafka client to check if its running
client_conn = KafkaClient(
    bootstrap_servers = "localhost:9092", # specific the broker address to connect to
    client_id = "Broker_id" # create id from the reference
)
# Check server is up and running
print(client_conn.bootstrap_connected())
# Check Kafka version number
print(client_conn.check_version())