FROM jupyter/pyspark-notebook:latest

USER root
RUN apt-get update && apt-get install -y curl

USER jovyan
# Ajout explicite de pyspark pour le path python
RUN pip install kafka-python requests hdfs pyspark

ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"