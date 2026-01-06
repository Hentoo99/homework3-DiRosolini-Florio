from confluent_kafka import Producer
import time
import json
import os

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

producer_config ={
    'bootstrap.servers': bootstrap_servers,
    'acks': 'all',
    'batch.size': 16000,
    'max.in.flight.requests.per.connection': 1,
    'retries': 5,
    'linger.ms': 100
}

class KafkaProducer:
    def __init__(self, topic):
        self.producer = Producer(producer_config)
        self.topic = topic

    def delivery_report(self, err, msg):
        """Callback to verify message delivery."""
        print("Inside delivery_report callback")
        if err:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def send_message(self, message):
        """Send a message to the Kafka topic."""
        self.producer.produce(
            self.topic,
            json.dumps(message, default=str).encode('utf-8'),
            callback=self.delivery_report
        )
        print("Message sent to Kafka topic.")
        print(message)
        self.producer.poll(0)
        self.producer.flush()

    def flush(self):
        """Flush the producer to ensure all messages are sent."""
        self.producer.flush()