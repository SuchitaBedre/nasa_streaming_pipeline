FROM apache/airflow:2.7.1

USER root

# 1. Install Java with aggressive network cache-busting configurations
RUN rm -rf /var/lib/apt/lists/* && \
    apt-get clean && \
    echo "Acquire::http::Pipeline-Depth \"0\";" > /etc/apt/apt.conf.d/99parallel && \
    echo "Acquire::http::No-Cache \"true\";" >> /etc/apt/apt.conf.d/99parallel && \
    echo "Acquire::BrokenProxy \"true\";" >> /etc/apt/apt.conf.d/99parallel && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 3. Bypass the Airflow pip wrapper by executing via the Python module directly
RUN python -m pip install --no-cache-dir pyspark==3.5.0

# Keep this at the end so Airflow core stays happy
USER airflow