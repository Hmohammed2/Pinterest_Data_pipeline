from pyspark.sql import SparkSession
import multiprocessing
import pyspark
import os
import yaml
from pyspark.sql.functions import col
from pyspark.sql.functions import *

# Adding the packages required to get data from S3 and write data to Cassandra
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell"

with open('config/s3_creds.yaml','r') as f:
    s3_creds = yaml.safe_load(f)
    print(type(s3_creds))

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

def read_from_s3(session):
    """Read data from s3 bucket"""
    # Read from the S3 bucket
    df = session.read.option("multiline","true").json('s3a://pinterest-data-bucket/*') 

    return df

def transform_data():
        """Do transformation on data using pyspark"""

        # replace error or empty data with Nones
        df = df.replace({'User Info Error': None}, subset = ['follower_count','poster_name']) \
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
        df= df.withColumn("downloaded", self.df["downloaded"].cast("int")) \
                        .withColumn("index", self.df["index"].cast("int")) 
        
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

        df.show(500)


if __name__ == "__main__":
    data = read_from_s3(session)
    print(data.show(10))