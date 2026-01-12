from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, max, min, struct, to_json

spark = SparkSession.builder \
    .appName("ClimaticRecords") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Lecture des données brutes depuis HDFS
raw_history = spark.read.json("hdfs://namenode:9000/hdfs-data/history/*/weather_history_raw.json")

# Transformation : Correction de l'accès aux champs (noms au lieu d'index)
history_flat = raw_history.select(
    col("latitude"), col("longitude"),
    explode(arrays_zip("hourly.time", "hourly.temperature_2m", "hourly.windspeed_10m")).alias("data")
).select(
    "latitude", "longitude",
    col("data.time").alias("time"),
    col("data.temperature_2m").alias("temperature"),
    col("data.windspeed_10m").alias("windspeed")
)

# Calcul des records
records = history_flat.groupBy("latitude", "longitude").agg(
    max("temperature").alias("record_hot"),
    min("temperature").alias("record_cold"),
    max("windspeed").alias("record_wind")
)

# Publication vers Kafka
records.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_records") \
    .save()

spark.stop()