from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import os

# Connexion HDFS
hdfs_client = InsecureClient('namenode:50070', user='hdfs')  # adapte l'URL si nécessaire

consumer = KafkaConsumer(
    'weather_transformed',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='latest',
    group_id='weather-hdfs'
)

for message in consumer:
    data = json.loads(message.value)
    country = data['country']
    city = data['city']
    
    # Création du chemin HDFS
    hdfs_path = f'/data/hdfs-data/{country}/{city}/alerts.json'
    
    # Vérifie si le fichier existe pour append
    if hdfs_client.status(hdfs_path, strict=False):
        # Lecture actuelle, ajout d'une ligne
        with hdfs_client.read(hdfs_path, encoding='utf-8') as f:
            current_data = json.load(f)
    else:
        current_data = []

    current_data.append(data)
    
    # Écriture dans HDFS
    with hdfs_client.write(hdfs_path, encoding='utf-8', overwrite=True) as f:
        json.dump(current_data, f)
    
    print(f"Écrit dans HDFS: {hdfs_path}")
