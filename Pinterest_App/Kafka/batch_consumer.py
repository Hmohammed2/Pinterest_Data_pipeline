import json
import yaml
import logging
import os
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer
from json import loads

# Get S3 bucket credential
with open('config/s3_creds.yaml','r') as f:
    s3_creds = yaml.safe_load(f)

cluster_consumer = KafkaConsumer(
    "Pinterest_data",
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: loads(message),
    auto_offset_reset = "earliest"
    )

cluster_consumer.subscribe(topics=["Pinterest_data"])
 
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

print(get_buckets_client())

s3_client = boto3.client("s3")

with open('data.json', 'rw', encoding='utf-8') as f:
    for message in cluster_consumer:
        json.dump(message.value, f)
