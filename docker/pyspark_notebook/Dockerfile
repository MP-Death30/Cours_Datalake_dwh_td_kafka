FROM jupyter/pyspark-notebook:latest

USER root
RUN apt-get update && apt-get install -y curl

USER jovyan
# Ajout de streamlit et plotly pour le dashboard
RUN pip install kafka-python requests hdfs pyspark streamlit plotly pandas