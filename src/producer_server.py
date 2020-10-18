from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):
    """Produces Kafka messages to a topic from JSON file
        :param input_file: JSON file for source of events
        :param topic: Kafka topic name to write events to
    """

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        """Opens JSON file and converts each record into a Kafka message"""
        with open(self.input_file) as f:
            json_data = json.load(f)
            for record in json_data:
                message = self.dict_to_binary(record)
                self.send(topic=self.topic, value=message)
                time.sleep(1)

    def dict_to_binary(self, json_dict):
        """Converts JSON object to UTF-8 encoded byte array"""
        return json.dumps(json_dict).encode('utf-8')
