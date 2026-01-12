import requests
import json
from hdfs import InsecureClient

def download_history(city, lat, lon):
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date=2015-01-01&end_date=2025-01-01&hourly=temperature_2m,windspeed_10m"
    data = requests.get(url).json()
    
    hdfs_client = InsecureClient('http://namenode:9870', user='root')
    path = f"/hdfs-data/history/{city}/weather_history_raw.json"
    
    hdfs_client.makedirs(f"/hdfs-data/history/{city}")
    with hdfs_client.write(path, overwrite=True, encoding='utf-8') as f:
        json.dump(data, f)
    print(f"Historique de {city} sauvegardé dans HDFS.")

if __name__ == "__main__":
    download_history("Paris", 48.85, 2.35)