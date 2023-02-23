FROM --platform=linux/x86-64 jupyter/pyspark-notebook:spark-3.3.1
RUN pip install openlineage-sql==0.20.6 openlineage-python==0.20.6
ADD openlineage-sql-java-0.20.6.jar .
ADD TestParser.class .