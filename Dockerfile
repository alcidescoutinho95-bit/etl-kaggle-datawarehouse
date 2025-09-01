FROM jupyter/all-spark-notebook:latest

COPY ./requirements.txt /tmp/requirements.txt

# Usa o pip do ambiente conda do notebook
RUN conda run -n python3 pip install -r /tmp/requirements.txt
