import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
from hdfs import InsecureClient
import plotly.express as px

# Configuration pour exécution INTERNE au réseau Docker
# Les noms 'kafka' et 'namenode' sont résolus par le DNS interne de Docker
KAFKA_SERVER = 'kafka:9092'
HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'root'

st.set_page_config(page_title="Météo Data Lake Dashboard", layout="wide")
st.title("🛰️ Dashboard Climatique - Architecture Lambda")

# --- SECTION 1 : LOGS HDFS (BATCH LAYER) ---
st.header("📊 Historique des Alertes (HDFS)")

try:
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    # Chemin définit lors de l'Exercice 7
    hdfs_path = '/hdfs-data/France/Paris/alerts.json'
    
    with client.read(hdfs_path, encoding='utf-8') as reader:
        # Lecture ligne par ligne du fichier JSON composite
        data = [json.loads(line) for line in reader if line.strip()]
    
    if data:
        df = pd.DataFrame(data)
        df['event_time'] = pd.to_datetime(df['event_time'])
        df = df.sort_values('event_time')

        # Affichage des métriques
        m1, m2, m3 = st.columns(3)
        m1.metric("Température Actuelle", f"{df['temperature'].iloc[-1]} °C")
        m2.metric("Vitesse Vent", f"{df['windspeed'].iloc[-1]} km/h")
        m3.metric("Total Enregistrements", len(df))

        # Graphiques d'évolution
        st.subheader("Évolution Temporelle")
        fig = px.line(df, x='event_time', y=['temperature', 'windspeed'], 
                      title="Température et Vent à Paris")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("En attente de données dans HDFS...")

except Exception as e:
    st.error(f"Erreur de lecture HDFS : {e}")

# --- SECTION 2 : ANOMALIES TEMPS RÉEL (SPEED LAYER) ---
# Consommation du topic weather_anomalies défini à l'Exercice 13
st.sidebar.header("🚨 Anomalies Détectées (Kafka)")

@st.cache_resource
def get_kafka_consumer():
    return KafkaConsumer(
        'weather_anomalies',
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=500  # Temps d'attente court pour ne pas bloquer l'UI
    )

try:
    consumer = get_kafka_consumer()
    # Récupération des derniers messages
    messages = []
    for msg in consumer:
        messages.append(msg.value)
    
    if messages:
        for anomaly in reversed(messages):
            with st.sidebar.expander(f"⚠️ {anomaly['city']} - {anomaly['event_time'][:16]}", expanded=True):
                st.write(f"**Variable :** {anomaly.get('variable', 'N/A')}")
                st.write(f"**Observé :** {anomaly.get('observed_value', anomaly.get('observed_temp'))}")
                st.write(f"**Attendu :** {anomaly.get('expected_value', anomaly.get('expected_temp')):.1f}")
                st.info(f"Type : {anomaly.get('anomaly_type', 'Écart significatif')}")
    else:
        st.sidebar.write("Aucune anomalie détectée pour le moment.")

except Exception as e:
    st.sidebar.error(f"Erreur Kafka : {e}")

# --- SECTION 3 : RECORDS HISTORIQUES (EXO 10) ---
st.divider()
st.header("🏆 Records Historiques (Batch Records)")
try:
    # Lecture du topic weather_records produit par batch_records.py
    record_consumer = KafkaConsumer(
        'weather_records',
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=500
    )
    
    for record in record_consumer:
        c1, c2, c3 = st.columns(3)
        c1.metric("Record Chaleur", f"{record['record_hot']} °C")
        c2.metric("Record Froid", f"{record['record_cold']} °C")
        c3.metric("Record Vent", f"{record['record_wind']} km/h")
        break # On n'affiche que le dernier record calculé
except:
    st.write("Calcul des records batch en cours...")