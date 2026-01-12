import sys
import json
from kafka import KafkaConsumer

if len(sys.argv) < 2:
    print("Erreur : Spécifiez le nom du topic.")
    sys.exit(1)

topic_name = sys.argv[1]

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

print(f"Écoute du topic '{topic_name}'...")
try:
    for message in consumer:
        print(f"Message reçu : {message.value}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()