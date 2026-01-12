from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, struct, to_json, abs
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("AnomalyDetector") \
    .getOrCreate()

# 1. Chargement des profils enrichis (Batch Layer - Exo 12)
# On utilise le répertoire généré par batch_profiling_enriched.py
profiles_df = spark.read.json("hdfs://namenode:9000/hdfs-data/France/Paris/seasonal_profile_enriched/*/")

# 2. Définition du schéma du flux (Exo 4/6)
stream_schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("event_time", TimestampType())
])

# 3. Lecture du flux temps réel
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_transformed") \
    .load()

weather_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), stream_schema).alias("data")) \
    .select("data.*") \
    .withColumn("month", month(col("event_time")))

# 4. Jointure avec la Batch Layer (Architecture Lambda)
enriched_stream = weather_stream.join(profiles_df, ["month"], "left")

# 5. Logique d'anomalie du PDF
# Vent: > 2 écarts-types | Température: écart > 5°C
anomalies = enriched_stream.filter(
    (abs(col("temperature") - col("temp_mean")) > 5) | 
    (col("windspeed") > (col("wind_mean") + 2 * col("wind_std"))) 
).select(
    col("city"), col("country"), col("event_time"),
    col("temperature").alias("observed_value"), # Pour le JSON de l'Exo 13
    col("temp_mean").alias("expected_value"),
    col("windspeed"),
    col("wind_mean"),
    col("wind_std")
)

# 6. Émission vers Kafka ET Sauvegarde HDFS
query = anomalies.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_anomalies") \
    .option("checkpointLocation", "/home/jovyan/work/checkpoints_anomalies") \
    .start()

query.awaitTermination() # INDISPENSABLE pour éviter l'arrêt immédiat