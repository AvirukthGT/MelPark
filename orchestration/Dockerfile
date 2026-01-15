FROM apache/airflow:2.8.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install providers for Azure, Kafka, and Postgres
RUN pip install --no-cache-dir \
    apache-airflow-providers-microsoft-azure \
    apache-airflow-providers-apache-kafka \
    psycopg2-binary \
    requests