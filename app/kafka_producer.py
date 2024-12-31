import json
import logging
from datetime import datetime
import faker
from kafka3 import KafkaProducer  # type: ignore
from kafka3.errors import KafkaError  # type: ignore

logging.basicConfig(level=logging.INFO)


class Producer:
    def __init__(self) -> None:
        self._init_kafka_producer()

    def _init_kafka_producer(self) -> None:
        self.kafka_host = "localhost:9092"
        self.kafka_topic = "input-topic"
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_host,
            value_serializer=lambda v: json.dumps(v).encode(),
        )

    def publish_to_kafka(self, message) -> None:
        try:
            self.producer.send(topic=self.kafka_topic, value=message)
            self.producer.flush()
        except KafkaError as ex:
            logging.error(f"Exception {ex}")
        else:
            logging.info(f"Published message {message} into topic {self.kafka_topic}")

    @staticmethod
    def create_random_email() -> dict:
        f = faker.Faker()

        new_contact = dict(
            username=f.user_name(),
            first_name=f.first_name(),
            last_name=f.last_name(),
            email=f.email(),
            date_created=str(datetime.utcnow()),
        )
        return new_contact


if __name__ == "__main__":
    producer = Producer()
    count = 0
    while count < 20:
        random_email = producer.create_random_email()
        producer.publish_to_kafka(random_email)
        count += 1
