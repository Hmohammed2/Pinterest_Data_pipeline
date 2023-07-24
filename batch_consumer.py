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

 