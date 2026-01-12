import requests
import json
import time
import sys
from kafka import KafkaProducer

GEO_URL = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_weather(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true"
    }
    resp = requests.get(WEATHER_URL, params=params)
    resp.raise_for_status()
    return resp.json().get("current_weather", {})

def get_coordinates(city_name):
    params = {"name": city_name, "count": 1, "language": "fr", "format": "json"}
    resp = requests.get(GEO_URL, params=params)
    resp.raise_for_status()
    results = resp.json().get("results")
    if not results:
        print(f"Erreur : Ville '{city_name}' non trouvée.")
        sys.exit(1)
    # Extraction des données requises pour le partitionnement HDFS [cite: 44]
    return {
        "lat": results[0]["latitude"],
        "lon": results[0]["longitude"],
        "city": results[0]["name"],
        "country": results[0].get("country", "Unknown")
    }

def main():
    if len(sys.argv) < 2:
        print("Usage: python weather_producer.py <nom_de_ville>")
        sys.exit(1)

    city_query = sys.argv[1]
    loc = get_coordinates(city_query)
    
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Producteur actif pour : {loc['city']}, {loc['country']} ({loc['lat']}, {loc['lon']})")

    try:
        while True:
            weather = fetch_weather(loc['lat'], loc['lon'])
            # Structure du message pour le consommateur HDFS
            payload = {
                "city": loc['city'],
                "country": loc['country'],
                "temperature": weather.get("temperature"),
                "windspeed": weather.get("windspeed"),
                "timestamp": time.time()
            }
            producer.send("weather_stream", payload)
            print(f"Envoyé : {payload}")
            time.sleep(10) # Fréquence augmentée pour le test
    except KeyboardInterrupt:
        producer.close()

if __name__ == "__main__":
    main()
