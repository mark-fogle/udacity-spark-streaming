""" Kafka Test Consumer"""
import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
import time
import json

KAFKA_TOPIC_NAME = "gov.sfpd.servicecalls"
BOOTSTRAP_SERVER = "localhost:9092"
GROUP_ID = "consumer_test"


def run_kafka_consumer():

    # Create Kafka Broker connection configuration
    broker_properties = {
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "group.id": GROUP_ID,
        'default.topic.config': {'auto.offset.reset': 'earliest'}
    }

    # Create Kafka consumer
    consumer = Consumer(
        broker_properties
    )

    # Subscribe to topic
    consumer.subscribe([KAFKA_TOPIC_NAME])

    # Poll message loop
    while True:
        num_results = 1
        while num_results > 0:
            message = consumer.poll(1)
            if message is None:
                num_results = 0
            elif message.error() is not None:
                num_results = 0
            else:
                json_data = json.loads(message.value().decode('utf-8'))
                print(json_data)
                num_results = 1
            time.sleep(1)
    consumer.close()


if __name__ == "__main__":
    run_kafka_consumer()
