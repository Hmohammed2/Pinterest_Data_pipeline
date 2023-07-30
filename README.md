# Pinterest Data Pipeline
Every day Pinterest runs thousands of experiments to determine what features to implement to improve the experience for their 450 million users and drive business value. These include image uploads and image clicks that will need to be processed to inform the decisions that they will make. This can be achieved with a reliable and scalable data pipeline, which i will be attempting to replicate in this project. I will be implementing a lambda architecture which will run two separate pipelines for both batch processing and real time streaming processing.using different techstacks. Batch processing provides a comprehensive and accurate view of historical data whereas the streaming pipeline will provide real time streaming processing to provide views of recent data.

## Table of Contents
* [1. Project Overview](#1-Project-overview)
* [2. Data Ingestion](#2-Data-Ingestion)
    * [2.1 Configuring the API](#21-Configuring-the-API)
    * [2.2 Consuming the data into Kafka](#22-Consuming-the-data-into-Kafka)
* [3. Batch Processing](#3-Batch-Processing)
   * [3.1 Ingest data into the lake](31-Ingest-data-into-the-lake)
   * [3.2 Process data into Spark](32-Process-data-into-Spark)
   * [3.3 Orchestrate the batch processing using Airflow](33-Orchestrate-the-batch-processing-using-Airflow)
* [4. Stream Processing](#4-Stream-Processing)

# 1. Project Overview
As mentioned in the introduction, this project will involve replicating Pinterests end to end data processing pipeline in Python. It will be implemented based on Lambda architecture that utilises both batch and stream processing.

To start off this involved creating an API and utilising Kafka to distribute the data to both an Amazon S3 bucket and Spark streaming.

For the real time processing, stream data was processed using structured streaming and saved onto a local PostgreSQL database for later analysis. For the batch processing side, batch data was extracted from the S3 bucket and transformed/cleaned in Spark which would be orchestrated via Airflow.

![alt text](https://github.com/Hmohammed2/Pinterest_Data_pipeline/blob/main/images/project-overview.png)

# 2. Data Ingestion
## 2.1 Configuring the API
The project includes emulating the live environment of the infrastructure similar to what a data engineer would work on in Pinterest. This includes an API listening for events made by users on the app, or developers request to the API. A user emulation script is also included to simulate users uploading data into the API.
## 2.2 Consuming the data into Kafka
kafka-python- was used for as the client that allows for you to interact with Apache Kafka in a pythonic way. It allows for you to write Python code to perform many of the Kafka tasks such as creating topics, produce data for the topics that are available via the terminal. 

Kafka-Python can be simply installed using pip just run `pip install kafka-python`. Once installed you will need to start your Kafka Broker and Zookeeper to begin interacting with Kafka. This was done via the terminal ubuntu

![alt text](https://github.com/Hmohammed2/Pinterest_Data_pipeline/blob/main/images/zookeeper-start.PNG)
![alt text](https://github.com/Hmohammed2/Pinterest_Data_pipeline/blob/main/images/kafka-start.PNG)

A topic was created which will be named "Pinterest_data".

```python
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id = "Kafka-Administrator"
)

topics = ["Pinterest_data"]

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
```
We then create a Kafka producer which can be used to send messages into the topic. These messages will be sent via the API.

```python
# Create Producer to send message to a kafka topic
kafka_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest data producer",
    value_serializer=lambda mlmessage: dumps(mlmessage).encode("ascii")
) n


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    kafka_producer.send(topic="Pinterest_data", value=data)
    return item
```
# 3. Batch Processing
## 3.1 Ingest data into the lake
The first step would be to create an S3 bucket which can be done via the AWS console. Once an S3 bucket was created, a script will have to be created to ensure a kafka batch processing consumer can recieve the data from the topic to be stored into the S3 bucket. The code can be found in the batch_consumer.py
```python
cluster_consumer = KafkaConsumer(
    "Pinterest_data",
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: loads(message),
    auto_offset_reset = "earliest"
    )

cluster_consumer.subscribe(topics=["Pinterest_data"])
 
s3_client = boto3.client("s3")

print(s3_client.list_buckets())

for message in cluster_consumer:
    
    json_object = json.dumps(message.value, indent=4)
    unique_id = (message.value)["unique_id"]
    filename = 'event-'+unique_id+'.json'
    filepath = os.path.join('events',filename)
    
    s3_client.put_object(
        Body=json_object,
        Bucket = s3_creds['Bucket'],
        Key = filepath)
```
Kafka-Python was used to extract the messages from the consumer.I then used boto3 to send the data received by the Kafka consumer to the S3 bucket created when it is consumed.
Each event was saved as a JSON file.The data will be kept in S3 for long-term persistent storage, and wait to be processed all at once by Spark batch processing jobs either run periodically by Airflow.

![alt text](https://github.com/Hmohammed2/Pinterest_Data_pipeline/blob/main/images/S3-bucket.PNG)

## 3.2 Process data into Spark
When processing data with Spark, we need to read data from different data sources, which in this case will be from our AWS S3 bucket. Therefore, this part first explains how to use pyspark to read data from AWS S3, and then use spark SQL for simple data processing. The code can be found in the batch_processing.py file.

## 3.3 Orchestrate the batch processing using Airflow


# 4. Stream Processing
