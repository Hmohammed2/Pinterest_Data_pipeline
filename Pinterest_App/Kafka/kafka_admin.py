from kafka.admin import NewTopic, KafkaAdminClient
from kafka import KafkaConsumer
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id = "Kafka-Administrator"
)

topics = ["topic1", "topic2", "topic3"]

def create_topics(topic_names):
    existing_topic_list = KafkaConsumer().topics()
    print(list(KafkaConsumer().topics()))
    topic_list = []
    for topic in topic_names:
        if topic not in existing_topic_list:
            print('Topic : {} added '.format(topic))
            topic_list.append(NewTopic(name=topic, num_partitions=3, replication_factor=1))
        else:
            print('Topic : {topic} already exist ')
    try:
        if topic_list:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topic Created Successfully")
        else:
            print("Topic Exist")
    except TopicAlreadyExistsError as e:
        print("Topic Already Exist")
    except  Exception as e:
        print(e)

def delete_topics(topic_names):
    try:
        admin_client.delete_topics(topics=topic_names)
        print("Topic Deleted Successfully")
    except UnknownTopicOrPartitionError as e:
        print("Topic Doesn't Exist")
    except  Exception as e:
        print(e)

if __name__ == "__main__":
    create_topics(topics)
    print(admin_client.list_topics())
