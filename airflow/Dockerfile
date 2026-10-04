ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.3.2
FROM ${AIRFLOW_IMAGE_NAME}

ARG AIRFLOW_VERSION=3.3.2

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt