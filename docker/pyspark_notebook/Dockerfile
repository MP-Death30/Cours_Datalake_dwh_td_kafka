FROM jupyter/pyspark-notebook:7a0c7325e470

USER root
RUN apt-get update && apt-get install -y curl

USER jovyan
# Installation des libs pour le producteur météo (Exo 3)
RUN pip install kafka-python requests

# Injection automatique des JARs Spark-Kafka pour le streaming (Exo 4)
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 pyspark-shell"