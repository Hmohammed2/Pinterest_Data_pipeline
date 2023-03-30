from kafka import KafkaConsumer
from json import loads
import json
import boto3
from botocore.exceptions import ClientError
import logging
import os

cluster_consumer = KafkaConsumer(
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: loads(message),
    auto_offset_reset = "earliest"
    )

cluster_consumer.subscribe(topics=["topic1"])

def get_buckets_client():
    session = boto3.session.Session()
    # User can pass customized access key, secret_key and token as well
    s3_client = session.client('s3')
    try:
        response = s3_client.list_buckets()
        buckets =[]
    
        for bucket in response['Buckets']:
            buckets += {bucket["Name"]}

    except ClientError:
        print("Couldn't get buckets.")
        raise
    else:
        return buckets

def upload_file(filename, bucket, object_name=None):
        """ Uploads a file onto a s3 bucket
        : param: filename = the file that is to be uploaded
        : param: bucket = the s3 bucket that it will be uploaded to
        : param: object_name = s3 object name, if unspecified default to filename
        """
        if object_name is None:
            object_name = os.path.basename(filename)

        # Upload the file
        s3_client = boto3.client("s3")
        try:
            response = s3_client.upload_file(filename, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

print(get_buckets_client())

with open('data.json', 'rw', encoding='utf-8') as f:
    for message in cluster_consumer:
        json.dump(message.value, f)
