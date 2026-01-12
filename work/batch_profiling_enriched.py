from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, month, avg, stddev, count, when
import datetime

spark = SparkSession.builder.appName("EnrichedProfiling").getOrCreate()

# 1. Lecture de l'historique HDFS
df_raw = spark.read.json("hdfs://namenode:9000/hdfs-data/history/*/weather_history_raw.json")

# 2. Aplatissement des données
df_flat = df_raw.select(
    explode(arrays_zip("hourly.time", "hourly.temperature_2m", "hourly.windspeed_10m")).alias("data")
).select(
    col("data.time").cast("timestamp").alias("time"),
    col("data.temperature_2m").alias("temp"),
    col("data.windspeed_10m").alias("wind")
)

# 3. Calcul des profils (Moyennes + Écarts-types pour l'Exo 13)
enriched_profiles = df_flat.withColumn("month", month(col("time"))) \
    .groupBy("month") \
    .agg(
        avg("temp").alias("temp_mean"),
        stddev("temp").alias("temp_std"),
        avg("wind").alias("wind_mean"),
        stddev("wind").alias("wind_std")
    )

# 4. Sauvegarde (Partitionnement Pays/Ville/Année)
year_str = str(datetime.datetime.now().year)
path = f"hdfs://namenode:9000/hdfs-data/France/Paris/seasonal_profile_enriched/{year_str}/"
enriched_profiles.write.mode("overwrite").json(path)
spark.stop()