import requests
import time
import os
from flask import jsonify
import circuitBreaker

CLIENT_ID = os.environ.get('OPENSKY_USERNAME')
CLIENT_SECRET = os.environ.get('OPENSKY_PASSWORD')

AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API_BASE_URL = "https://opensky-network.org/api"

cb = circuitBreaker.CircuitBreaker(failure_threshold=3, recovery_timeout=1)

def get_access_token():
  
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERRORE: Credenziali mancanti nelle variabili d'ambiente.")
        return None

    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    
    try:
        response = requests.post(AUTH_URL, data=payload, timeout=10)
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.exceptions.RequestException as e:
        print(f"ERRORE Autenticazione OAuth2: {e}")
        if response.text:
            print(f"Dettaglio errore server: {response.text}")
        return None
    
def chiamata(url, params=None, headers=None, timeout=10):
    print(f"Effettuo chiamata a {url} con params {params} e headers {headers}")
    response = requests.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response
    

def opensky_api_request(airport_code, hours_back, flight_type):
    token = get_access_token()
    print("Access token ottenuto:")
    print(token)
    print(f"Flight type: {flight_type}, Airport: {airport_code}, Hours back: {hours_back}")
    if not token:
        return []
    end_time = int(time.time()) - 1200
    print(f"End time: {end_time}")
    start_time = end_time - (hours_back * 3600)

    chunk_size = 8 * 3600 

    timeSv = end_time
    flights = []
    while timeSv > start_time:
        sv = timeSv - chunk_size
    
        if sv < start_time:
            print("Aggiustamento start time chunk")
            sv = start_time


        url = f"{API_BASE_URL}/flights/{flight_type}"
        params = {
            'airport': airport_code,
            'begin': sv,
            'end': timeSv
        }
        headers = {
            'Authorization': f"Bearer {token}"
        }
        print("Effettuando chiamata OpenSky API:")
        print(url, params, headers)
        try:
            response = cb.call(chiamata, url, params=params, headers=headers, timeout=15)
            data = response.json()
            print(f"Ricevuti {len(data)} voli dal chunk {sv} to {timeSv}")
            flights.extend(data)
        except Exception as e:
            print(f"Errore durante la chiamata OpenSky API: {e}")

        timeSv = sv
    print(f"Totale voli recuperati: {len(flights)}")
    return flights

def get_departures_by_airport(airport_code, hours_back=24):
    return opensky_api_request(airport_code, hours_back, "departure")


def get_arrivals_by_airport(airport_code, hours_back=24):
    return opensky_api_request(airport_code, hours_back, "arrival")

if __name__ == "__main__":
    voli = get_arrivals_by_airport('LICC', hours_back=6) 
    print(f"Voli trovati: {len(voli)}")