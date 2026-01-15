FROM apache/airflow:2.8.1

USER root

# 1. Install System Dependencies (curl is needed to download Docker)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Install the LATEST Docker CLI (Manually)
# We download the official binary to ensure we match your host's version
ENV DOCKER_VERSION=26.0.0
RUN curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" -o docker.tgz \
    && tar xzvf docker.tgz \
    && mv docker/docker /usr/local/bin/ \
    && rm -rf docker docker.tgz

# 3. Create the 'docker' group and add airflow user
# We create the group manually with GID 999 (standard) or just add the user
RUN groupadd -g 999 docker || true && \
    usermod -aG docker airflow

USER airflow

# 4. Install Python Providers
RUN pip install --no-cache-dir \
    apache-airflow-providers-microsoft-azure \
    apache-airflow-providers-apache-kafka \
    psycopg2-binary \
    requests