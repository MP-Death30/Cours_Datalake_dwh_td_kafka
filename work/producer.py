import requests
import json
import time
import sys
from kafka import KafkaProducer

def fetch_weather(lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=True"
    response = requests.get(url)
    return response.json()

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Coordonnées (Paris par défaut)
latitude = sys.argv[1] if len(sys.argv) > 1 else 48.85
longitude = sys.argv[2] if len(sys.argv) > 2 else 2.35

print(f"Démarrage du producteur pour Lat:{latitude} Lon:{longitude}...")
try:
    while True:
        data = fetch_weather(latitude, longitude)
        producer.send('weather_stream', data)
        print(f"Données envoyées : {data['current_weather']}")
        time.sleep(10)
except KeyboardInterrupt:
    producer.close()