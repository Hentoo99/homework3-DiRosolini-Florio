from confluent_kafka import Consumer, KafkaError
import json
import time
import os
import smtplib
import ssl
from email.message import EmailMessage


BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = 'to-notifier'
GROUP_ID = 'alert_notifier_group'

SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com') 
SMTP_PORT = int(os.getenv('SMTP_PORT', '465'))          
SENDER_EMAIL = os.getenv('SENDER_EMAIL', 'gabrieleflorio18@gmail.com')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD', 'cugo drbr livg hefd')

consumer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

def wait_for_kafka():
    print(f"Tentativo di connessione a Kafka su: {BOOTSTRAP_SERVERS}...")
    while True:
        try:
            temp_consumer = Consumer(consumer_config)
            temp_consumer.list_topics(timeout=5.0)
            temp_consumer.close()
            print("--- KAFKA È PRONTO! Connessione stabilita. ---")
            return
        except Exception:
            print("Kafka non è ancora pronto... riprovo tra 5 secondi.")
            time.sleep(5)

def send_email(data):
    msg = EmailMessage()
    print(data)
    print(type(data))
    msg['Subject'] = f"NOTIFICA: {data.get('airport', 'Un aeroporto')} ha molti voli!"
    msg['From'] = SENDER_EMAIL
    msg['To'] = data.get('user')
    
    content = f"""
    Ciao,
    è stata rilevata una variazione significativa nel numero di voli presso l'aeroporto {data.get('airport', 'unknown')}.
    Valore di interesse attuale: {data.get('interestValue', 'unknown')}, ha superato le tue soglie impostate.
    In particolare: {data.get('condition', 'unknown')}
    Ti consigliamo di controllare i dettagli.
    Saluti,
    Il team di Alert System
    """
    msg.set_content(content)

    context = ssl.create_default_context()

    try:
        print(f"Tentativo invio email a {data.get('user')}...")
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        print("--- EMAIL INVIATA CON SUCCESSO ---")
        return True
    except Exception as e:
        print(f"ERRORE invio email: {e}")
        return False

class KafkaConsumerWrapper:
    def __init__(self, topic):
        self.consumer = Consumer(consumer_config)
        self.topic = topic
        self.consumer.subscribe([self.topic])

    def consume_messages(self):
        msg = self.consumer.poll(1.0)
        if msg is None: return None
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")
            return None
        try:
            val_utf8 = msg.value().decode('utf-8')
            return json.loads(val_utf8)
        except Exception as e:
            print(f"Errore decodifica: {e}")
            return None 

    def commit_offsets(self):
        self.consumer.commit()
    
    def close(self):
        self.consumer.close()

if __name__ == "__main__":
    wait_for_kafka()
    print("Inizializzazione Kafka Consumer Wrapper con modulo Email...")
    consumer = KafkaConsumerWrapper(TOPIC)
    
    try:
        while True:
            message_data = consumer.consume_messages()
            if message_data:
                print(f"--- MESSAGGIO RICEVUTO ---")
                email_sent = send_email(message_data)
                if email_sent:
                    print("Email inviata con successo.")
                consumer.commit_offsets() 
                print("Offset committato.")
                print("--------------------------")        
    except KeyboardInterrupt:
        print("Arresto manuale ricevuto.")
    finally:
        consumer.close()