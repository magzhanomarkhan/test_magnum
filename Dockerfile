FROM apache/airflow:latest
USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get -y install chromium chromium-driver && \
    apt-get clean

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER airflow
