from confluent_kafka import Consumer, KafkaError, Producer
import json
import os
import time
import sys

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = 'to-alert-system'
GROUP_ID = 'alert_system_group'

consumer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

producer_config ={
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'acks': 'all',
    'batch.size': 16000,
    'max.in.flight.requests.per.connection': 1,
    'retries': 5,
    'linger.ms': 100
}

def wait_for_kafka():
    """
    Ciclo che tenta di connettersi a Kafka. 
    Blocca l'esecuzione finché Kafka non è pronto.
    """
    print(f"Tentativo di connessione a Kafka su: {BOOTSTRAP_SERVERS}...")
    while True:
        try:
            temp_consumer = Consumer(consumer_config)
            temp_consumer.list_topics(timeout=5.0)
            temp_consumer.close()
            print("--- KAFKA È PRONTO! Connessione stabilita. ---")
            return
        except Exception as e:
            print("Kafka non è ancora pronto... riprovo tra 5 secondi.")
            time.sleep(5)

class KafkaProducerWrapper:
    def __init__(self, topic):
        self.producer = Producer(producer_config)
        self.topic = topic

    def delivery_report(self, err, msg):
        """Callback to verify message delivery."""
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
        self.producer.poll(0)
        self.producer.flush()

    def flush(self):
        """Flush the producer to ensure all messages are sent."""
        self.producer.flush()

class KafkaConsumerWrapper:
    def __init__(self, topic):
        self.consumer = Consumer(consumer_config)
        self.topic = topic
        self.consumer.subscribe([self.topic])

    def consume_messages(self):
        msg = self.consumer.poll(1.0)
        
        if msg is None:
            return None
        
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")
            return None

        try:
            val_utf8 = msg.value().decode('utf-8')
            try:
                value = json.loads(val_utf8)
            except json.JSONDecodeError:
                value = val_utf8 
                
            return value
        except Exception as e:
            print(f"Errore generico decodifica: {e}")
            return None

    def commit_offsets(self):
        self.consumer.commit()

    def close(self):
        self.consumer.close()

if __name__ == "__main__":
    wait_for_kafka()
    print("Inizializzazione Kafka Consumer Wrapper...")
    consumer = KafkaConsumerWrapper(TOPIC)
    producer = KafkaProducerWrapper('to-notifier')
    print(f"Consumer avviato. In ascolto sul topic: '{TOPIC}'")
    
    try:
        while True:
            message = consumer.consume_messages()
            if message:
                print(f"--- MESSAGGIO RICEVUTO ---")
                print(message)
                print("--------------------------")

                for airport in message['data']:
                    airArrSize = len(message['data'][airport]['arrivals'])
                    airDepSize = len(message['data'][airport]['departures'])
                    val = airArrSize + airDepSize
                    print(f"Valore totale per aeroporto {airport}: {val}")
                    print(message['data'][airport]['users'])
                    print(type(message['data'][airport]['users']))
                    for user in message['data'][airport]['users']:
                        print(user)
                        print(user['highValue'] < val)
                        print(user['lowValue'] > val)
                        if user['highValue'] < val or user['lowValue'] > val:
                            if user['highValue'] < val:
                                condition = f'highValue = {user["highValue"]}'
                            else:
                                condition = f'lowValue = {user["lowValue"]}'
                            print(user)
                            email_body = f"""
                                Gentile Utente,
                                Variazione significativa voli a {airport}.
                                Valore attuale: {val}.
                                Dettaglio: {condition}.
                                """
                            producer.send_message({
                                "to": user['email'],
                                "subject": f"ALERT VOLI: {airport}",
                                "body": email_body
                            })
                        else:
                            print(f"Nessuna notifica inviata per l'utente {user['email']} all'aeroporto {airport}.")
                            
                consumer.commit_offsets()
    except KeyboardInterrupt:
        print("Arresto manuale ricevuto.")
    finally:
        consumer.close()