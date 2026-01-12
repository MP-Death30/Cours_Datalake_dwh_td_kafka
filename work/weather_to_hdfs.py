from kafka import KafkaConsumer
from hdfs import InsecureClient
import json

# Connexion HDFS (Port 9870 pour Hadoop 3)
hdfs_client = InsecureClient('http://namenode:9870', user='root')

consumer = KafkaConsumer(
    'weather_transformed',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consommateur HDFS démarré et en attente de données...")
for message in consumer:
    data = message.value
    country = data.get('country', 'Unknown')
    city = data.get('city', 'Unknown')
    
    hdfs_path = f'/hdfs-data/{country}/{city}/alerts.json'
    
    # Création récursive des répertoires parents
    hdfs_client.makedirs(f'/hdfs-data/{country}/{city}')
    
    # Vérification : le fichier existe-t-il déjà ?
    file_exists = hdfs_client.status(hdfs_path, strict=False) is not None
    
    # Écriture : Création si nouveau (append=False), Ajout si existant (append=True)
    try:
        with hdfs_client.write(hdfs_path, append=file_exists, encoding='utf-8') as f:
            f.write(json.dumps(data) + "\n")
        
        status = "Append" if file_exists else "Création"
        print(f"[{status}] Alerte archivée dans HDFS : {hdfs_path}")
    except Exception as e:
        print(f"Erreur lors de l'écriture HDFS : {e}")