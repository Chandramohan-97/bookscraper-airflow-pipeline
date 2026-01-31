FROM apache/airflow:2.10.0
USER root
COPY requirements.txt /
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
