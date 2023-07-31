import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, BinaryType
from pyspark.sql.functions import from_json, col

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.5.0 pyspark-shell'
# specify the topic we want to stream data from.
kafka_topic_name = "Pinterest_data"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

with open('config/postgresql_creds.yaml','r') as f:
    psql_creds = yaml.safe_load(f)
    print(type(psql_creds))

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# construct schema for streaming dataframe object
json_schema = StructType()\
        .add("category", StringType())\
        .add("index", IntegerType())\
        .add("unique_id", StringType())\
        .add("title", StringType())\
        .add("description", StringType())\
        .add("follower_count", StringType())\
        .add("tag_list", StringType())\
        .add("is_image_or_video", StringType())\
        .add("image_src", StringType())\
        .add("downloaded", IntegerType())\
        .add("save_location", StringType())   


# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df\
    .withColumn("value", from_json(stream_df["value"].cast("string"), json_schema))\
    .select(col("value.*"))

# Clean the data before writing to stream
stream_df = stream_df.select(
                'unique_id',
                'index',
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

stream_df = stream_df.withColumnRenamed("index", "index_id")
# replace error or empty data with Nones

stream_df = stream_df.replace({'User Info Error': None}, subset = ['follower_count']) \
                         .replace({'No description available Story format': None}, subset = ['description']) \
                         .replace({'No description available': None}, subset = ['description']) \
                         .replace({'Image src error.': None}, subset = ['image_src'])\
                         .replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}, subset = ['tag_list'])\
                         .replace({'Image src error.': None}, subset = ['image_src'])\
                         .replace({"No Title Data Available": None}, subset = ['title'])

def _write_to_postgres(stream_df, epoch_id)-> None:
    
    stream_df.write \
        .mode('append') \
        .format("jdbc") \
        .option("url", psql_creds["URL"]) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", psql_creds["DBTABLE"]) \
        .option("user", psql_creds["USER"]) \
        .option("password", psql_creds["PASSWORD"]) \
        .save()

# outputting the messages to the console 
stream_df.writeStream \
    .foreachBatch(_write_to_postgres) \
    .start() \
    .awaitTermination()
