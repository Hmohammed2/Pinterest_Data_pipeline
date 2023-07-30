# Pinterest Data Pipeline
Every day Pinterest runs thousands of experiments to determine what features to implement to improve the experience for their 450 million users and drive business value. These include image uploads and image clicks that will need to be processed to inform the decisions that they will make. This can be achieved with a reliable and scalable data pipeline, which i will be attempting to replicate in this project. I will be implementing a lambda architecture which will run two separate pipelines for both batch processing and real time streaming processing.using different techstacks. Batch processing provides a comprehensive and accurate view of historical data whereas the streaming pipeline will provide real time streaming processing to provide views of recent data.

## Table of Contents
* [1. Project Overview](#1.-Project-overview)
* [2. Data Ingestion](#1.-Data-Ingestion)

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

![alt text](https://github.com/Hmohammed2/Pinterest_Data_pipeline/blob/main/images/zookeeper-start.png)
![alt text](https://github.com/Hmohammed2/Pinterest_Data_pipeline/blob/main/images/kafka-start.png)
