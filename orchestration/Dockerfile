FROM apache/airflow:2.8.1

USER root

# 1. Install System Dependencies
# Added default-jdk because PySpark requires Java to run the JVM
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Install the LATEST Docker CLI (Manually)
ENV DOCKER_VERSION=26.0.0
RUN curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" -o docker.tgz \
    && tar xzvf docker.tgz \
    && mv docker/docker /usr/local/bin/ \
    && rm -rf docker docker.tgz

# 3. Create the 'docker' group and add airflow user
RUN groupadd -g 999 docker || true && \
    usermod -aG docker airflow

# Set JAVA_HOME environment variable (standard path for default-jdk)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# 4. Install Python Providers + PySpark
# Added pyspark to match your Spark Cluster version (3.5.0)
RUN pip install --no-cache-dir \
    apache-airflow-providers-microsoft-azure \
    apache-airflow-providers-apache-kafka \
    psycopg2-binary \
    requests \
    pyspark==3.5.0