FROM python:3.11-slim-bullseye

USER root

# Install Java 17
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Spark 3.5.1
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

# Download Delta Lake JARs directly into Spark's jars folder
# This makes them available on the classpath WITHOUT needing --packages
ENV DELTA_VERSION=3.1.0
ENV SCALA_VERSION=2.12

RUN curl -fsSL \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
    -o "${SPARK_HOME}/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    -o "${SPARK_HOME}/jars/delta-storage-${DELTA_VERSION}.jar"

# Install Python dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    websocket-client==1.7.0 \
    requests==2.31.0 \
    vaderSentiment==3.3.2 \
    pyspark==3.5.1 \
    delta-spark==3.1.0 \
    python-dotenv==1.0.0 \
    yfinance==0.2.36

WORKDIR /app
