# Pinterest Data Pipeline
Every day Pinterest runs thousands of experiments to determine what features to implement to improve the experience for their 450 million users and drive business value. These include image uploads and image clicks that will need to be processed to inform the decisions that they will make. This can be achieved with a reliable and scalable data pipeline, which i will be attempting to replicate in this project. I will be implementing a lambda architecture which will run two separate pipelines for both batch processing and real time streaming processing.using different techstacks. Batch processing provides a comprehensive and accurate view of historical data whereas the streaming pipeline will provide real time streaming processing to provide views of recent data.

## Table of Contents
* [1. Project Overview](#1-Project-overview)
* [2. Data Ingestion](#2-Data-Ingestion)
    * [2.1 Configuring the API](#21-Configuring-the-API)
    * [2.2 Consuming the data into Kafka](#22-Consuming-the-data-into-Kafka)
* [3. Batch Processing](#3-Batch-Processing)
   * [3.1 Ingest data into the lake](#31-Ingest-data-into-the-lake)
   * [3.2 Process data into Spark](#32-Process-data-into-Spark)
   * [3.3 Orchestrate the batch processing using Airflow](#33-Orchestrate-the-batch-processing-using-Airflow)
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

We can start by first trying to integrate spark with AWS S3. This can be done by adding the appropriate application library (jars). The libraries contain the dependencies needed to act as connectors. They can be found on the Maven depository https://mvnrepository.com/ and are specified in the format groupId:artifactId:version.

Once the library has been fully configured we can then configure the context (access ID and access key) in the SparkSession for authentication. For extra security the information is stored in a variable within a yaml file.
```python
cfg = (
    pyspark.SparkConf()
    # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("TestApp")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
)

sc = pyspark.SparkContext(conf=cfg)

access_key = s3_creds['aws_access_key_id']
secret_access_key = s3_creds['aws_secret_access_key']

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', access_key)
hadoopConf.set('fs.s3a.secret.key', secret_access_key)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') 

session = SparkSession(sc).Builder().getOrCreate()
```

The data can then be processed using Spark SQL module. Spark SQL lets you query structured data inside Spark programs, using either SQL or a familiar DataFrame API. Spark SQL operates through lazy evaluation which means Spark will not start the execution of the process until an ACTION is called. This has several advantages such as increased speed and reduced computation as there is less calculation overhead. 

```python
def transform_data(df):
        """Do some transformation/cleaning on the data using pyspark.sql functions"""

        # replace error or empty data with Nones
        df = df.replace({'User Info Error': None}, subset = ['follower_count']) \
                         .replace({'No description available Story format': None}, subset = ['description']) \
                         .replace({'No description available': None}, subset = ['description']) \
                         .replace({'Image src error.': None}, subset = ['image_src'])\
                         .replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}, subset = ['tag_list'])\
                         .replace({'Image src error.': None}, subset = ['image_src'])\
                         .replace({"No Title Data Available": None}, subset = ['title']) \
        
        # Convert the unit to corresponding zeros
        df = df.withColumn("follower_count", when(col('follower_count').like("%k"), regexp_replace('follower_count', 'k', '000')) \
                        .when(col('follower_count').like("%M"), regexp_replace('follower_count', 'M', '000000'))\
                        .cast("int"))

        # Convert the type into int
        df= df.withColumn("downloaded", df["downloaded"].cast("int")) \
                        .withColumn("index", ddf["index"].cast("int")) 
        
        # Rename the column
        df = df.withColumnRenamed("index", "index_id")

        # reorder columns
        df = df.select('unique_id',
                                'index_id',
                                'title',
                                'category',
                                'description',
                                'follower_count',
                                'tag_list',
                                'is_image_or_video',
                                'image_src',
                                'downloaded',
                                'save_location'
                                )
    
    # show the first 20 rows of data into the terminal
        df.show(20)
```
## 3.3 Orchestrate the batch processing using Airflow
Apache Airflow is a task-orchestration tool that allows you to define a series of tasks to be executed in a specific order. The tasks can be run in a distributed manner using Airflow's scheduler.

In Airflow you use Directed Acyclic Graphs (DAGs) to define a workflow. Each node in the DAG corresponds to a task, and they will be connected to one another.DaGS can be created via the Airflow UI or through a python script which we will be doing in the script spark_processing_jon.py. The DAG will periodically initiate both the scripts batch_consumer.py and batch_processing.py once a day at 10am.

```python
with DAG(dag_id='batch_processing',
         default_args=default_args,
         schedule='0 10 * * *',
         catchup=False,
         ) as dag:

    batch_consume_task = BashOperator(
        task_id='consume_batch_data',
        bash_command=f'cd {work_dir} && python batch_consumer.py '
    )
    batch_process_task = BashOperator(
        task_id='process_batch_data',
        bash_command=f'cd {work_dir} && python batch_processing.py'
    )

    
    batch_consume_task >> batch_process_task
```
# 4. Stream Processing
For streaming data, this project uses Spark Structured Streaming to consume data from our Kafka topic in real time, then process the data, and finally send the streaming data into our local PostgreSQL database for storage.Similarly, Spark requires appropriate additional libraries (jars) and configuration information to integrate Kafka and PostgreSQL.The code can be found in the "stream_processing.py"

```python
# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.5.0 pyspark-shell'
```

```python
# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()
```
We can then write the data we have retrieved from our topic into the local PostgreSQL database as shown in the screenshot below.

![alt text](https://github.com/Hmohammed2/Pinterest_Data_pipeline/blob/main/images/schema.PNG)

After initating the query on our data base SELECT unique_id, title FROM experimental_streaming. We can see that the data has successfully been stored into our local database.
![alt text](https://github.com/Hmohammed2/Pinterest_Data_pipeline/blob/main/images/PostgreSQL-expression.PNG)

