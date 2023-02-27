FROM jupyter/pyspark-notebook:spark-3.3.1
USER root
RUN apt update && apt install -y jq curl
USER jovyan
RUN pip install openlineage-sql==0.20.6 openlineage-python==0.20.6
ADD openlineage-sql-java-0.21.0-SNAPSHOT.jar .
ADD TestParser.class .