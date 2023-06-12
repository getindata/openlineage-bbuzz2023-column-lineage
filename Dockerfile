FROM jupyter/pyspark-notebook:spark-3.3.1
USER root
RUN apt update && apt install -y jq curl
USER jovyan
RUN pip install openlineage-sql==0.27.2 openlineage-python==0.27.2 pyspark==3.3.1
ADD TestParser.class .