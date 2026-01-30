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
ADMIN_EMAIL = os.getenv('ADMIN_EMAIL', 'gabrieleflorio01@gmail.com')
T_SCRAPE= 15
T_CHECK = 5*T_SCRAPE 
history = {} 
breach_counts = Counter()
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
    global metrics
    try:
        with open(SLA_CONFIG_PATH, 'r') as file:
            sla_config = yaml.safe_load(file)
            print("Configurazione SLA caricata correttamente.")
            metrics = sla_config.get('metrics', [])
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
    return None,None

def trigger_sla_violation(metric_name, value, min_val, max_val, breach_type):
    email_body = f"""
    Gentile Amministratore,
    è stata rilevata una violazione SLA per {metric_name}.
    Valore: {value} (Consentito: {min_val}-{max_val}).
    Tipo violazione: {breach_type}.
    Timestamp: {time()}
    """
    message = {
        "to": ADMIN_EMAIL, 
        "subject": f"CRITICO: Violazione SLA {metric_name}",
        "body": email_body
    }

    print(f"prova: {kafka_producer}")
    print(f"prova2: {message}")
    if kafka_producer:
        kafka_producer.send_message(message)
    print(f"!!! SLA VIOLATION: {metric_name} val={value} ({breach_type}) !!!")

def loop_check():
    print(f"Avvio loop di controllo SLA ogni {T_CHECK} secondi...")
    counter_low= Counter()
    counter_high = Counter()
    sv = Counter()
    
    windows_size = 5
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
            if current_val is None and current_timestamp is None:
                continue
            print("--------------------------\n\n")
            print(f"Valore attuale peeeeeer {name}: {current_val}")
            
            if current_val is not None:
                if name not in history:
                    history[name] = deque(maxlen=10)
                history[name].append({"current_val": current_val, "current_timestamp": current_timestamp})
                print("history", history[name])
                print(f"Valore nella storia: {current_val}, min={min_v}, max={max_v}")
                print(f"Valore di inlunghezza vett{sv[name]}")


                if current_val < min_v:
                    counter_low[name] += 1
                    values_violated[name]['low'].append({"current_val": current_val, "current_timestamp": current_timestamp})
                if current_val > max_v:
                    counter_high[name] +=1
                    values_violated[name]['high'].append({"current_val": current_val, "current_timestamp": current_timestamp})
                    print(f"incremento counter high = {counter_high[name]}\n")
                print(f"Contatori per {name}: BELOW_MIN={counter_low[name]}, ABOVE_MAX={counter_high[name]}")
                print("\n\nresto:",sv[name]%5)


                if counter_low[name] >= 3:
                    print(f"Trigger SLA violation per {name} BELOW_MIN")
                    low_values = [item["current_val"] for item in values_violated[name]['low']]
                    trigger_sla_violation(name, low_values, min_v, max_v, "BELOW_MIN")
                    counter_low[name] = 0
                    breach_counts[name] += 1
                    values_violated[name]['low'].clear()
                    sv[name] = 0
                elif counter_high[name] >= 3:
                    print(f"Trigger SLA violation per {name} ABOVE_MAX")    
                    high_values = [item["current_val"] for item in values_violated[name]['high']]
                    trigger_sla_violation(name, high_values, min_v, max_v, "ABOVE_MAX")
                    counter_high[name] = 0
                    breach_counts[name] += 1
                    values_violated[name]['high'].clear()
                    sv[name] = 0
                sv[name] += 1
                print(f"Valore di fine vett{sv[name]}")
                if sv[name]%5 == 0:
                    counter_low[name] = 0
                    counter_high[name] = 0
                    sv[name] = 0
            else:
                print(f"Nessun dato per {name}")
        sleep(T_CHECK)


def mia():
    print("ciao")

@app.route("/update_sla", methods=["POST"])
def update_config():
    data = flask.request.get_json()
    min_val = data.get("min")
    max_val = data.get("max")
    metric_name = data.get("metric")
    query = data.get("query")
    print(f"Received update_sla request: metric={metric_name}, min={min_val}, max={max_val}")
    if metric_name is None or min_val is None or max_val is None :
        return flask.jsonify({"error": "Missing parameters"}), 400
    found = False
    for m in metrics:
        print("prima della modifica metric", m)  
        if m['name'] == metric_name:
            found = True
            m['min'] = min_val
            m['max'] = max_val
            m['query'] = query
        print("dopo la modifica metric", m)    

    
    if not found:
        return flask.jsonify({"error": "Metric not found in SLA configuration"}), 404
    return flask.jsonify({"message": "SLA configuration updated successfully"}), 200

@app.route("/read_sla", methods=["POST"])
def read_sla():
    data = flask.request.get_json()
    if "metric" in data:
        for m in metrics:
            if m['name'] == data.get("metric"):
                print("configurazione corrente:", m)
                return flask.jsonify(m), 200
        return flask.jsonify({"error": "Metric not found in SLA configuration"}), 404
    return flask.jsonify(metrics), 200

@app.route("/breach_stats", methods=["POST"])
def check_status():
    data = flask.request.get_json()
    if data:
        for m in metrics:
            if m['name'] == data.get("metric"):
                name = m['name']
                if name in history and breach_counts[name] > 0:
                    return flask.jsonify({f"Breach per metrica: {name}": breach_counts[name]}), 200
                else:
                    return flask.jsonify({"error": "No databreach for this metric"}), 404
    return flask.jsonify({"Breach per metrica": breach_counts}), 200

if __name__ == "__main__":
    load_sla_config()

    kafka_producer = KafkaProducerWrapper(TOPIC)
    monitor_thread = threading.Thread(target=loop_check, daemon=True)
    monitor_thread.start()

    app.run(host="0.0.0.0", port=5000, debug=False)