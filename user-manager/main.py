import flask
import mysql.connector
import time
import os
import sys
from concurrent import futures
import grpc
import threading
sys.path.append(os.path.join(os.getcwd(), 'proto')) # FONDAMENTALE

import user_manager_pb2       
import user_manager_pb2_grpc  
import data_collector_pb2
import data_collector_pb2_grpc
from prometheus_client import start_http_server, Gauge, Counter

DB_HOST = 'db'
DB_USER = 'root'
DB_PASSWORD = 'root_password'
DB_NAME = 'user_db'


NODE_NAME = os.getenv('NODE_NAME', 'unknown_node')
SERVICE_NAME = 'user_manager'
DB_UPDATE_DURATION = Gauge('user_manager_db_update_duration_seconds', 'Duration of user update in seconds', ['service','node'])

TOTAL_USER = Gauge('total_user', 'Total number of user inserted', ['service','node'])
ERRORS = Counter('user_manager_errors_total', 'Total number of errors in User Manager service', ['service','node'])

CPU_USAGE = Gauge('user_manager_cpu_usage_percent', 'CPU usage percentage of User Manager service', ['service','node'])

GRPC_CHECKS = Counter('user_manager_grpc_checks_total', 'Total number of gRPC user existence checks', ['service','node'])

cache= {}
cache_lock = threading.Lock()

def start_metrics_server():
    start_http_server(8000)
    print("Prometheus metrics server started on port 8000", flush=True)

class UserManagerServicer(user_manager_pb2_grpc.UserManagerServicer):
    def CheckUserExists(self, request, context):
        GRPC_CHECKS.labels(service=SERVICE_NAME, node=NODE_NAME).inc()
        print("gRPC request to check if user exists")
        data = {'email': request.email}
        exists = get_is_inserted(data)
        print(f"User exists: {exists}")
        return user_manager_pb2.CheckUserExistsResponse(exists=exists)
app = flask.Flask(__name__)


def get_is_inserted(data):
    db_conn = get_db_connection()
    try:
        if db_conn.is_connected():
            cursor = db_conn.cursor()
            QUERY = "SELECT * FROM users WHERE email = %s"
            valori = (data['email'], )
            cursor.execute(QUERY, valori)
            result = cursor.fetchone()
            if result:
                print("User already exists in the database")
                return True
        print("User does not exist in the database")
        return False
    finally:
        if db_conn.is_connected():
            db_conn.close()


def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_manager_pb2_grpc.add_UserManagerServicer_to_server(UserManagerServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC server started on port 50051", flush=True)
    server.wait_for_termination()

def get_db_connection():
    retries = 30
    while retries > 0:
        try:
            print(f"Tentativo di connessione a {DB_HOST}...", flush=True)
            conn = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            print("--- CONNESSO AL DB CON SUCCESSO ---", flush=True)
            return conn
        except mysql.connector.Error as err:
            ERRORS.labels(service=SERVICE_NAME, node=NODE_NAME).inc()
            print(f"Errore DB ({err}). Riprovo tra 5 secondi...", flush=True)
            retries -= 1
            time.sleep(5)
    raise Exception("Impossibile connettersi al database dopo vari tentativi.")

#db_conn = get_db_connection()

@app.route('/')
def home():
    return flask.jsonify({"message": "Welcome to the User Manager API", "db_status": "Connected"})

@app.route('/add_user', methods=['POST'])
def add_user():
    with DB_UPDATE_DURATION.labels(service=SERVICE_NAME, node=NODE_NAME).time():
        db_conn = get_db_connection()
        try:
            global cache
            print("Received data for new user")
            data = flask.request.json
            request_id = data['request_id'] 
            print(f"Processing request ID: {request_id}")
            print(f"Cache: {cache}")
            print(f"Request ID: {request_id}")
            if not request_id:
                    ERRORS.labels(service=SERVICE_NAME, node=NODE_NAME).inc()
                    return flask.jsonify({"status": "Missing request_id"}), 400
            with cache_lock:
                if request_id in cache:
                    ERRORS.labels(service=SERVICE_NAME, node=NODE_NAME).inc()
                    return flask.jsonify({"status": "Duplicate request", "user": cache[request_id]})
            
            if(db_conn.is_connected()):
                if not get_is_inserted(flask.request.json):
                    print("Adding new user to the database")
                    cursor =  db_conn.cursor()
                    
                    QUERY = "INSERT INTO users (email, name, surname, age, CF, phone) VALUES (%s, %s, %s, %s, %s, %s)"
                    valori = (data['email'], data['name'], data['surname'], data['age'], data['CF'], data['phone'])
                    cursor.execute(QUERY, valori)
                    db_conn.commit()
                    if cursor.rowcount > 0:
                        response = "User added successfully"
                        
                        TOTAL_USER.labels(service=SERVICE_NAME, node=NODE_NAME).inc()
                    with cache_lock:
                        cache[request_id] = {
                            "response": response,
                            "timestamp": time.time()
                        }
                    return flask.jsonify({"status": response, "user": data})
                return flask.jsonify({"status": "User already exists", "user": flask.request.json})
            return flask.jsonify({"status": "DB not connected"})
        finally:
            if db_conn.is_connected():
                db_conn.close()



@app.route('/get_user', methods=['POST'])
def get_user():
    db_conn = get_db_connection()
    try:
        print("Received request to get user")
        if(db_conn.is_connected()):
            cursor =  db_conn.cursor(dictionary=True)
            data = flask.request.json
            QUERY = "SELECT * FROM users WHERE email = %s"
            valori = (data['email'], )
            cursor.execute(QUERY, valori)
            result = cursor.fetchone()
            if result:
                print("User retrieved successfully")
                return flask.jsonify({"status": "User found", "user": result})
            return flask.jsonify({"status": "User not found", "email": data['email']})
        return flask.jsonify({"status": "DB not connected"})
    finally:
        if db_conn.is_connected():
            db_conn.close()
    
def removeInterests(email):
    print(f"Removing interests for user: {email}")
    with grpc.insecure_channel('data-collector:50052') as channel:
        stub = data_collector_pb2_grpc.DataCollectorStub(channel)
        response = stub.RemoveInterestbyUser(data_collector_pb2.UserRequest(email=email))
        if not response.success:
            return False
    return True
@app.route('/rmv_user', methods=['POST'])
def rmv_user():
    with DB_UPDATE_DURATION.labels(service=SERVICE_NAME, node=NODE_NAME).time():
        db_conn = get_db_connection()
        try:
            print("Received request to remove user")
            if(db_conn.is_connected()):
                print("Checking if user exists for removal")
                if get_is_inserted(flask.request.json):
                    cursor =  db_conn.cursor()
                    data = flask.request.json
                    QUERY = "DELETE FROM users WHERE email = %s"
                    valori = (data['email'], )
                    cursor.execute(QUERY, valori)
                    db_conn.commit()
                    if cursor.rowcount > 0:
                        TOTAL_USER.labels(service=SERVICE_NAME, node=NODE_NAME).dec()
                        if removeInterests(data['email']):
                            print("User removed successfully")
                            return flask.jsonify({"status": "User removed", "email": data['email']})
                        return flask.jsonify({"status": "User doesn't have interests, but is removed", "email": data['email']})
                    return flask.jsonify({"status": "User not removed", "email": data['email']})
                return flask.jsonify({"status": "User does not exist", "email": flask.request.json['email']})
            return flask.jsonify({"status": "DB not connected"})
        finally:
            if db_conn.is_connected():
                db_conn.close()

if __name__ == '__main__':
    start_metrics_server()

    CPU_USAGE.labels(service=SERVICE_NAME, node=NODE_NAME).set_function(lambda: os.getloadavg()[0] * 10)

    server = threading.Thread(target=run_grpc_server)
    server.start()
    app.run(host='0.0.0.0', port=5000, debug=False)