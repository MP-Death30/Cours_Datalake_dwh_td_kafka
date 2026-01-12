from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, min, max, count, when
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("WeatherStreamingAggregates") \
    .getOrCreate()

# Schéma correspondant au nouveau topic weather_transformed
schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_transformed") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Agrégats sur fenêtre glissante de 5 min toutes les 1 min [cite: 167]
windowed_agg = parsed.groupBy(
    window(col("event_time"), "5 minutes", "1 minute"),
    col("city"), col("country")
).agg(
    avg("temperature").alias("avg_temp"),
    min("temperature").alias("min_temp"),
    max("temperature").alias("max_temp"),
    # Compte des alertes critiques [cite: 169]
    count(when(col("wind_alert_level") != "level_0", 1)).alias("count_wind_alerts"),
    count(when(col("heat_alert_level") != "level_0", 1)).alias("count_heat_alerts")
)

query = windowed_agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()