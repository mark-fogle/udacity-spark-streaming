# Udacity: San Francisco Crime Statistics with Spark Streaming

## Project Overview

This project provides a real-world dataset, extracted from Kaggle, on San Francisco crime incidents. Statistical analyses of the data is performed using Apache Spark Structured Streaming. The project will create a Kafka server to produce data, and ingest data through Spark Structured Streaming.

## Requirements

* Spark 2.4.3
* Scala 2.11.x
* Java 1.8.x
* Kafka build with Scala 2.11.x
* Python 3.6.x or 3.7.x

## Kafka Producer Server

The Kafka producer [kafka_server.py](/src/kafka_server.py) for this project takes a JSON file see [police-department-calls-for-service.zip](/data/police-department-calls-for-serviuce.zip) with San Franciso crime statistics data and writes each police service call record to a Kafka topic.

```sh
python kafka-producer.py
```

This is tested using both the kafka-console-consumer CLI utility as well as a custom test Kafka consumer written in Python (consumer_server.py).

### Kafka Console Consumer

![Kafka Console Consumer](/docs/images/KafkaConsoleConsumer.png)

### Python Consumer Server

![Kafka Console Consumer](/docs/images/consumer_server.png)

## Apache Spark Streaming with Kafka

The last part of this project is to ingest data through Spark Structured Streaming. The python file (data_stream.py) contains the PySpark code to consume events from the Kafka topic, aggregate them over a given window and write the to a new stream on the console.

![Spark Streaming Output](/docs/images/spark_streaming.png)

Progress Reporter

![Progress Reporter](/docs/images/progress_report.png)

Spark UI

![Spark UI](/docs/images/spark_ui.png)

## Questions

* How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Changing the readStream maxOffsetsPerTrigger allowed for more records to be processed within a batch. This increases the total latency per batch but also the total throughput.

* What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Adjusting maxOffsetsPerTrigger, and the window duration and slide duration affetcs how many records are processed per batch. More records per batch is more efficient to a degreee sur to the overhead of processing each batch vs. processing records.
