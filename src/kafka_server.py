import producer_server

KAFKA_TOPIC_NAME = "gov.sfpd.servicecalls"
BOOTSTRAP_SERVER = "localhost:9092"
CLIENT_ID = "producer_server"


def run_kafka_server():
    """Writes police department service call events to Kafka topic from JSON file"""
    input_file = "../data/police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=KAFKA_TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVER,
        client_id=CLIENT_ID
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
