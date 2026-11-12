FROM apache/airflow:latest

USER root
# Устанавливаем Java (нужна для работы Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Устанавливаем PySpark и драйвер ClickHouse (если используется clickhouse-connect или JDBC)
RUN pip install --no-cache-dir pyspark clickhouse-connect
