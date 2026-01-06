import flask
import sys, atexit, threading
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
import collector
import os, time
import grpc
from concurrent import futures
import kafkaProducer as kp
sys.path.append(os.path.join(os.getcwd(), 'proto')) 
import user_manager_pb2  
import user_manager_pb2_grpc
import data_collector_pb2
import data_collector_pb2_grpc

            

print("Starting Data Collector Service...")
app = flask.Flask(__name__)
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(MONGO_URI)
db = client.flight_data_db
days = 1

users_interests_collection = db.interests   
flights_collection_arrival = db.flights_arrival
flights_collection_departure = db.flights_departure
producer = kp.KafkaProducer(topic='to-alert-system')
print("Connected to MongoDB.")

class DataCollectorServicer(data_collector_pb2_grpc.DataCollectorServicer):
    def RemoveInterestbyUser(self, request, context):
        print("gRPC request to remove user interest")
        email = request.email
        response = users_interests_collection.delete_many({'email': email})
        
        if response.deleted_count == 0:
            print(f"No interests found for user: {email}")
            return data_collector_pb2.DataCollectorResponse(success = False)
        print(f"Removed {response.deleted_count} interests for user: {email}")
        return data_collector_pb2.DataCollectorResponse(success = True)
    


def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_collector_pb2_grpc.add_DataCollectorServicer_to_server(DataCollectorServicer(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("gRPC server started on port 50052", flush=True)
    server.wait_for_termination()


def check_user_exists(email):
    print(f"Verifying user existence for email: {email} via gRPC")
    with grpc.insecure_channel('user-manager:50051') as channel:
        stub = user_manager_pb2_grpc.UserManagerStub(channel)
        response = stub.CheckUserExists(user_manager_pb2.CheckUserExistsRequest(email=email))
        print(f"User existence response: {response}")
        if not response.exists:
            return False
        print("User exists.")
    return True
def update_flight_data():
    print("--- Avvio aggiornamento ciclico voli ---")
    airports = users_interests_collection.distinct("airport_code")
    if not airports:
        print("Nessun aeroporto da monitorare.")
        return
    sv ={}
    for airport in airports:
        print(f"Scaricamento dati per: {airport}...")
       
       
        flightsArrival = collector.get_arrivals_by_airport(airport, hours_back=24*days)
        if flightsArrival:
            try:
                for f in flightsArrival:
                    f['airport_monitored'] = airport
                print(f"volo in arrivi: {flightsArrival}")
                flights_collection_arrival.insert_many(flightsArrival)
                print(f"Salvati {len(flightsArrival)} voli arrivo per {airport}")
            except Exception as e:
                print(f"Errore salvataggio Mongo arrival: {e}")
        
        flightsDepartures = collector.get_departures_by_airport(airport, hours_back=24*days)
        if flightsDepartures:
            try:
                for f in flightsDepartures:
                    f['airport_monitored'] = airport
                flights_collection_departure.insert_many(flightsDepartures)
                print(f"Salvati {len(flightsDepartures)} voli partenza per {airport}")
            except Exception as e:
                print(f"Errore salvataggio Mongo departures: {e}")
        sv[airport] = {
            "arrivals": flightsArrival,
            "departures": flightsDepartures,
            'users' : list(users_interests_collection.find({'airport_code': airport}))
        }
    producer.send_message({"status": "success", "message": "Flight data updated", "data": sv})


def checkInterestExists(email, airport_code):
    result = users_interests_collection.find_one({'email': email, 'airport_code': airport_code})
    return result is not None


scheduler = BackgroundScheduler()
scheduler.add_job(func=update_flight_data, trigger="interval", hours=12)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())


@app.route('/add_interest', methods=['POST'])
def add_interest():
    print("Received request to add user interest via REST API")
    data = flask.request.get_json()
    email = data.get('email')
    airport_code = data.get('airport_code')
    highValue = data.get('highValue')
    lowValue = data.get('lowValue')

    if lowValue is None and highValue is None:
        return flask.jsonify({'status': 'error', 'message': 'At least one of lowValue or highValue must be provided'}), 400
    else:
        if lowValue is None:
            lowValue = 0
        if highValue is None:
            highValue = 0
        
        if lowValue > highValue and lowValue != 0 and highValue != 0:
            return flask.jsonify({'status': 'error', 'message': 'lowValue must be less than highValue'}), 400
           

    if not email or not airport_code:
        return flask.jsonify({'status': 'error', 'message': 'Email and airport_code are required'}), 400

    print(f"Verifying user existence for email: {email} via gRPC")
    if not check_user_exists(email):
        return flask.jsonify({'status': 'error', 'message': 'User does not exist'}), 404
    print(f"Adding interest for email: {email}, airport_code: {airport_code}")
    result = users_interests_collection.update_one(
        {'email': email, 'airport_code': airport_code, 'lowValue': lowValue, 'highValue': highValue},
        {'$set': {'email': email, 'airport_code': airport_code, 'lowValue': lowValue, 'highValue': highValue}},
        upsert=True
    )
    
    if result.upserted_id:
        print(f"Inserted new interest with id: {result.upserted_id}")
        return flask.jsonify({'status': 'success', 'message': f'Interest for {airport_code} added for {email}'}), 200


    return flask.jsonify({'status': 'Failed already added', 'message': f'Interest for {airport_code} added for {email}'}), 409

@app.route('/rmv_interest', methods=['POST'])
def rmv_interest():
    print("Received request to remove user interest via REST API")
    data = flask.request.get_json()
    email = data.get('email')
    airport_code = data.get('airport_code')

    if not email or not airport_code:
        return flask.jsonify({'status': 'error', 'message': 'Email and airport_code are required'}), 400

    if not check_user_exists(email):
        return flask.jsonify({'status': 'error', 'message': 'User does not exist'}), 404
    
    result = users_interests_collection.delete_one({'email': email, 'airport_code': airport_code})

    if result.deleted_count == 0:
        return flask.jsonify({'status': 'error', 'message': 'No such interest found'}), 404

    return flask.jsonify({'status': 'success', 'message': f'Interest for {airport_code} removed for {email}'}), 200

@app.route('/list_interests', methods=['POST'])
def list_interests():
    print("Received request to list user interests via REST API")
    data = flask.request.get_json()
    email = data.get('email')
    print(f"Listing interests for email: {email}")
    if not email:
        return flask.jsonify({'status': 'error', 'message': 'Email is required'}), 400

    if not check_user_exists(email):
        return flask.jsonify({'status': 'error', 'message': 'User does not exist'}), 404

    interests = list(users_interests_collection.find({'email': email}, {'_id': 0, 'airport_code': 1, 'lowValue': 1, 'highValue': 1}))
    print(f"Found interests: {interests}")
    sv = {}
    for interest in interests:
        airport_codes = {'airport_code': interest['airport_code'],'lowValue': interest['lowValue'],'highValue': interest['highValue']}
        sv[interest['airport_code']] = airport_codes
        print(airport_codes)
    
    return flask.jsonify({'status': 'success', 'interests': sv}), 200

@app.route('/modify_interest_param', methods=['POST'])
def modify_interest_param():
    print("Received request to modify user interest parameters via REST API")
    data = flask.request.get_json()
    email = data.get('email')
    airport_code = data.get('airport_code')
    highValue = data.get('highValue')
    lowValue = data.get('lowValue')

    if not email or not airport_code:
        return flask.jsonify({'status': 'error', 'message': 'Email and airport_code are required'}), 400

    if lowValue is None and highValue is None:
        return flask.jsonify({'status': 'error', 'message': 'At least one of lowValue or highValue must be provided'}), 400
    else:
        if lowValue is None:
            lowValue = 0
        if highValue is None:
            highValue = 0
        
        if lowValue > highValue and lowValue != 0 and highValue != 0:
            return flask.jsonify({'status': 'error', 'message': 'lowValue must be less than highValue'}), 400

    if not check_user_exists(email):
        return flask.jsonify({'status': 'error', 'message': 'User does not exist'}), 404

    result = users_interests_collection.update_one(
        {'email': email, 'airport_code': airport_code},
        {'$set': {'lowValue': lowValue, 'highValue': highValue}}
    )

    if result.matched_count == 0:
        return flask.jsonify({'status': 'error', 'message': 'No such interest found'}), 404

    return flask.jsonify({'status': 'success', 'message': f'Interest parameters for {airport_code} updated for {email}'}), 200
    


@app.route('/get_flight', methods=['POST'])
def get_flight():
    print("Received request to get flight data via REST API")
    data = flask.request.get_json()
    email = data.get('email')
    airport_code = data.get('airport_code')
    if not check_user_exists(email):
        return flask.jsonify({'status': 'error', 'message': 'User does not exist'}), 404
    
    
    json_interests = []

    print(f"Fetching flight data for email: {email}, airport_code: {airport_code if airport_code else 'ALL'}")
    if not airport_code:
        print("No specific airport_code provided, fetching for all user interests.")
        interests = list(users_interests_collection.find({'email': email}, {'_id': 0, 'airport_code': 1}))
        for interest in interests:
            airport_code = interest['airport_code']
            flights = list(flights_collection_arrival.find({'airport_monitored': airport_code}))
            for flight in flights:
                flight['_id'] = str(flight['_id'])
            json_interests.append(flights)
            flights = list(flights_collection_departure.find({'airport_monitored': airport_code}))
            for flight in flights:
                flight['_id'] = str(flight['_id'])
                print(flight)
            json_interests.append(flights)
        return flask.jsonify({'status': 'success', 'flights': json_interests}), 200
    
    if not checkInterestExists(email, airport_code):
        return flask.jsonify({'status': 'error', 'message': 'Interest for this airport does not exist for the user'}), 404
    
    flights = list(flights_collection_arrival.find({'airport_monitored': airport_code}))
    for flight in flights:
        flight['_id'] = str(flight['_id'])
    json_interests.append(flights)
    flights = list(flights_collection_departure.find({'airport_monitored': airport_code}))
    for flight in flights:
        flight['_id'] = str(flight['_id'])
    json_interests.append(flights)
    return flask.jsonify({'status': 'success', 'flights': json_interests}), 200

@app.route('/force_update', methods=['POST'])
def force_update_flight_data():
    print("Forzato aggiornamento dati voli.")
    update_flight_data()
    return flask.jsonify({'status': 'success', 'message': 'Flight data update triggered'}), 200

@app.route('/get_arrivals', methods=['POST'])
def get_arrivals():
    print("Received request to get arrivals via REST API")
    data = flask.request.get_json()
    email = data.get('email')
    airport_code_arrivals = data.get('airport_code_arrivals')
    airport_code_departures = data.get('airport_code_departures')
    print(f"Requested arrivals for airport_code_arrivals: {airport_code_arrivals}, airport_code_departures: {airport_code_departures} by email: {email}")
    if not check_user_exists(email):
        return flask.jsonify({'status': 'error', 'message': 'User does not exist'}), 404
    print(f"Checking interest for email: {email}, airport_code_arrivals: {airport_code_arrivals}")
    if not checkInterestExists(email, airport_code_arrivals):
        return flask.jsonify({'status': 'error', 'message': 'Interest for this airport does not exist for the user'}), 404
    
    print(f"Fetching arrivals for email: {email}, airport_code_arrivals: {airport_code_arrivals}, airport_code_departures: {airport_code_departures}")
    flightsarrival = list(flights_collection_arrival.find({
    'airport_monitored': airport_code_arrivals,
    'estDepartureAirport': airport_code_departures
    }))
    print(flightsarrival)
    if not flightsarrival:
        return flask.jsonify({'status': 'error', 'message': 'No arrival flight data found for this airport'}), 404
    for flight in flightsarrival:
        flight['_id'] = str(flight['_id'])
    return flask.jsonify({'status': 'success', 'flights_arrival': flightsarrival}), 200

@app.route('/get_last_flight', methods=['POST'])
def get_last_flight():
    print("Received request to get last flight data via REST API")
    data = flask.request.get_json()
    email = data.get('email')
    airport_code = data.get('airport_code')
    print(f"Requested last flight for airport_code: {airport_code} by email: {email}")
    if not check_user_exists(email):
        print("User does not exist")
        return flask.jsonify({'status': 'error', 'message': 'User does not exist'}), 404
    
    if not checkInterestExists(email, airport_code):
        return flask.jsonify({'status': 'error', 'message': 'Interest for this airport does not exist for the user'}), 404

    print(f"Fetching last flight data for email: {email}, airport_code: {airport_code}")
    flightsarrival = flights_collection_arrival.find_one({'airport_monitored': airport_code}, sort=[('lastseen', -1)])
    save = []
    if flightsarrival:
        flightsarrival['_id'] = str(flightsarrival['_id'])
        save.append(flightsarrival)

    flightsDepartures = flights_collection_departure.find_one({'airport_monitored': airport_code}, sort=[('lastseen', -1)])
    if flightsDepartures:
        flightsDepartures['_id'] = str(flightsDepartures['_id'])
        save.append(flightsDepartures)

    if len(save) == 0:
        return flask.jsonify({'status': 'error', 'message': 'No flight data found for this airport'}), 404
    return flask.jsonify({'status': 'success', 'flights': save}), 200   


@app.route('/average', methods=['POST'])
def get_average_flights():
    print("Received request to get average flight data via REST API")
    data = flask.request.get_json()
    email = data.get('email')
    airport_code = data.get('airport_code')
    days = data.get('days', 1)

    if not check_user_exists(email):
        return flask.jsonify({'status': 'error', 'message': 'User does not exist'}), 404
    
    if not checkInterestExists(email, airport_code):
        return flask.jsonify({'status': 'error', 'message': 'Interest for this airport does not exist for the user'}), 404
    
    print(f"Calculating average flights for email: {email}, airport_code: {airport_code}, days: {days}")
    seconds_back = days * 24 * 3600
    current_time = int(time.time())
    start_time = current_time - seconds_back
    flight_count_arrival = flights_collection_arrival.count_documents({
        'airport_monitored': airport_code,
        'firstSeen': {'$gte': start_time}
    })

    average_per_day_arrival = flight_count_arrival / days

    flight_count_departure = flights_collection_departure.count_documents({
        'airport_monitored': airport_code,
        'firstSeen': {'$gte': start_time}
    })
    average_per_day_departure = flight_count_departure / days
    return flask.jsonify({'status': 'success', 'average_flights_per_day_arrival': average_per_day_arrival ,  'average_flights_per_day_departure': average_per_day_departure }), 200

if __name__ == '__main__':
    grpc_thread = threading.Thread(target=run_grpc_server)
    grpc_thread.start()

    app.run(host='0.0.0.0', port=5000, debug=True)

