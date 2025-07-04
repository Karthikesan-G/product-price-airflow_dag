FROM apache/airflow:3.0.2

USER root
RUN pip install --no-cache-dir apache-airflow-providers-mysql
USER airflow
