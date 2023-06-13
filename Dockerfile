FROM jupyter/pyspark-notebook:spark-3.3.1
USER root
RUN apt update && apt install -y jq curl
USER jovyan
RUN pip install jupyter_contrib_nbextensions
RUN jupyter contrib nbextension install --user
RUN jupyter nbextension enable collapsible_headings/main
RUN jupyter nbextension enable code_font_size/main
RUN jupyter nbextension enable codefolding/main
RUN pip install openlineage-sql==0.27.2 openlineage-python==0.27.2 pyspark==3.3.1
ADD TestParser.class .