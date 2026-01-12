from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, DoubleType

spark = SparkSession.builder \
    .appName("WeatherProcessor") \
    .getOrCreate()

# Schéma JSON Open-Meteo
schema = StructType([
    StructField("current_weather", StructType([
        StructField("temperature", DoubleType()),
        StructField("windspeed", DoubleType())
    ]))
])

# Lecture du flux Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_stream") \
    .load()

# Transformation
parsed_df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.current_weather.*")

processed_df = parsed_df.withColumn("event_time", current_timestamp()) \
    .withColumn("wind_alert_level", 
        when(col("windspeed") < 10, "level_0")
        .when(col("windspeed") <= 20, "level_1")
        .otherwise("level_2")) \
    .withColumn("heat_alert_level",
        when(col("temperature") < 25, "level_0")
        .when(col("temperature") <= 35, "level_1")
        .otherwise("level_2"))

# Écriture vers Kafka (weather_transformed)
query = processed_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_transformed") \
    .option("checkpointLocation", "/home/jovyan/work/checkpoints") \
    .start()

query.awaitTermination()