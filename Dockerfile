FROM apache/airflow:3.0.3
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir \
    clickhouse-driver \
    apache-airflow-providers-postgres==5.6.0 \
    airflow-clickhouse-plugin \
    apache-airflow-providers-telegram==4.0.0
