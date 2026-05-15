# Pin to bullseye (Debian 11) — stable, has openjdk-17, well tested
FROM python:3.11-slim-bullseye

USER root

# Install Java 17
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Spark 3.5.1
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

# Install all Python dependencies (producers + Spark jobs)
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    websocket-client==1.7.0 \
    requests==2.31.0 \
    vaderSentiment==3.3.2 \
    delta-spark==2.1.0 \
    pyspark==3.5.1 \
    python-dotenv==1.0.0 \
    yfinance==0.2.36

WORKDIR /app

# Copy producers and spark jobs
COPY producers/ ./producers/
COPY spark/ ./spark/
