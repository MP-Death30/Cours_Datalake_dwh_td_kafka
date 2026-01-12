from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, month, avg, count, when

spark = SparkSession.builder.appName("SeasonalProfiling").getOrCreate()

df_raw = spark.read.json("hdfs://namenode:9000/hdfs-data/history/*/weather_history_raw.json")

# Aplatissement : Correction de l'accès aux champs
df_flat = df_raw.select(
    col("latitude"), col("longitude"),
    explode(arrays_zip("hourly.time", "hourly.temperature_2m", "hourly.windspeed_10m")).alias("data")
).select(
    col("latitude"), col("longitude"),
    col("data.time").cast("timestamp").alias("time"),
    col("data.temperature_2m").alias("temp"),
    col("data.windspeed_10m").alias("wind")
)

# Calcul des profils mensuels
profiles = df_flat.withColumn("month", month(col("time"))) \
    .groupBy("latitude", "longitude", "month") \
    .agg(
        avg("temp").alias("temp_mean"),
        avg("wind").alias("wind_mean"),
        (count(when((col("temp") > 35) | (col("wind") > 20), 1)) / count("*")).alias("alert_probability")
    )

# Sauvegarde HDFS
profiles.write.mode("overwrite").json("hdfs://namenode:9000/hdfs-data/seasonal_profile/")

spark.stop()