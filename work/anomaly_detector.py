from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, current_timestamp, struct, to_json, abs
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("AnomalyDetector") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# 1. Chargement des profils historiques (Batch Layer) depuis HDFS
profiles_df = spark.read.json("hdfs://namenode:9000/hdfs-data/seasonal_profile/")

# 2. Définition du schéma pour le flux temps réel
stream_schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("event_time", TimestampType())
])

# 3. Lecture du flux temps réel (Speed Layer)
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_transformed") \
    .load()

# 4. Parsing et préparation de la jointure
weather_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), stream_schema).alias("data")) \
    .select("data.*") \
    .withColumn("month", month(col("event_time")))

# 5. Jointure Flux / Profils
# Note: On arrondit les coordonnées si nécessaire pour la jointure
enriched_stream = weather_stream.join(profiles_df, ["month"], "left")

# 6. Logique de détection d'anomalies
anomalies = enriched_stream.filter(
    (abs(col("temperature") - col("temp_mean")) > 5) |  # Écart > 5°C
    (col("windspeed") > (col("wind_mean") * 2))         # Vent > 2x la moyenne
).select(
    col("city"), col("country"), col("event_time"),
    col("temperature").alias("observed_temp"),
    col("temp_mean").alias("expected_temp"),
    col("windspeed").alias("observed_wind"),
    col("wind_mean").alias("expected_wind")
)

# 7. Publication des anomalies vers Kafka
query = anomalies.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_anomalies") \
    .option("checkpointLocation", "/home/jovyan/work/checkpoints_anomalies") \
    .start()

query.awaitTermination()