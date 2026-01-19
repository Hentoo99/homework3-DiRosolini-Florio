from time import time, sleep
import flask
import yaml
import os
import json
import requests
import threading
from collections import deque
from confluent_kafka import Producer
from collections import Counter

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = 'to-notifier' 
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
SLA_CONFIG_PATH = os.getenv("SLA_CONFIG_PATH", "/app/config/sla_config.yaml")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "20"))
T_SCRAPE= 5
T_CHECK = 5*T_SCRAPE 
number_errors = 3
history = {} 

app = flask.Flask(__name__)

producer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': 'sla-detector',
    'retries': 5
}

kafka_producer = None

class KafkaProducerWrapper:
    def __init__(self, topic):
        self.producer = Producer(producer_config)
        self.topic = topic

    def delivery_report(self, err, msg):
        if err:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, message):
        try:
            self.producer.produce(
                self.topic,
                json.dumps(message, default=str).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"Error producing Kafka message: {e}")

    def flush(self):
        self.producer.flush()

def load_sla_config():
    global sla_config
    try:
        with open(SLA_CONFIG_PATH, 'r') as file:
            sla_config = yaml.safe_load(file)
            print("Configurazione SLA caricata correttamente.")
            if 'metrics' in sla_config:
                for m in sla_config['metrics']:
                    history[m['name']] = deque(maxlen=100) 
    except Exception as e:
        print(f"Errore caricamento config: {e}")
        sla_config = {"metrics": []}

def query_prometheus(query):
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
        data = response.json()
        if data['status'] == 'success' and len(data['data']['result']) > 0:
            return float(data['data']['result'][0]['value'][1]),float(data['data']['result'][0]['value'][0])
    except Exception as e:
        print(f"Errore query Prometheus per {query}: {e}")
    return None

def trigger_sla_violation(metric_name, value, min_val, max_val, breach_type):
    message = {
        "type": "SLA_VIOLATION",
        "metric": metric_name,
        "value": value,
        "min": min_val,
        "max": max_val,
        "timestamp": time(),
        "violation_type": breach_type
    }
    print(f"prova: {kafka_producer}")
    print(f"prova2: {message}")
    if kafka_producer:
        kafka_producer.send_message(message)
    print(f"!!! SLA VIOLATION: {metric_name} val={value} ({breach_type}) !!!")

def loop_check():
    print(f"Avvio loop di controllo SLA ogni {CHECK_INTERVAL} secondi...")
    sleep(10)
    counter_low= Counter()
    counter_high = Counter()
    sv = Counter()
    metrics = sla_config.get('metrics', [])

    values_violated = {}
    for m in metrics:
        values_violated[m['name']] = {'low': deque(), 'high': deque()}
    while True:
        
        
        for m in metrics:
            name = m['name']
            query = m['query']
            min_v = m['min']
            max_v = m['max']

            current_val, current_timestamp = query_prometheus(query)
            print("--------------------------\n\n")
            print(f"Valore attuale peeeeeer {name}: {current_val}")
            
            if current_val is not None:
                print(f"Valore di inizo vett{sv[name]}")
                if name not in history:
                    history[name] = deque(maxlen=10)
                history[name].append({"current_val": current_val, "current_timestamp": current_timestamp})
                print("history", history[name])
               
                print(f"Valore nella storia: {current_val}, min={min_v}, max={max_v}")
                if current_val < min_v:
                    counter_low[name] += 1
                    values_violated[name]['low'].append({"current_val": current_val, "current_timestamp": current_timestamp})
                if current_val > max_v:
                    counter_high[name] +=1
                    values_violated[name]['high'].append({"current_val": current_val, "current_timestamp": current_timestamp})
                    print(f"incremento counter high = {counter_high[name]}\n")
                print(f"Contatori per {name}: BELOW_MIN={counter_low[name]}, ABOVE_MAX={counter_high[name]}")
            
                if counter_low[name] >= 3:
                    print(f"Trigger SLA violation per {name} BELOW_MIN")
                    trigger_sla_violation(name, values_violated[name]['low'], min_v, max_v, "BELOW_MIN")
                    counter_low[name] = 0
                    values_violated[name]['low'].clear()
                elif counter_high[name] >= 3:
                    print(f"Trigger SLA violation per {name} ABOVE_MAX")    
                    trigger_sla_violation(name, values_violated[name]['high'], min_v, max_v, "ABOVE_MAX")
                    counter_high[name] = 0
                    values_violated[name]['high'].clear()
                sv[name] = len(history[name])
                print(f"Storia dei valori: {history[name]}")
            else:
                print(f"Nessun dato per {name}")

        sleep(T_CHECK)

# --- AVVIO ---

if __name__ == "__main__":
    load_sla_config()

    kafka_producer = KafkaProducerWrapper(TOPIC)
    # Avvia il loop in un thread separato altrimenti Flask blocca tutto
    monitor_thread = threading.Thread(target=loop_check, daemon=True)
    monitor_thread.start()

    app.run(host="0.0.0.0", port=5000, debug=False)