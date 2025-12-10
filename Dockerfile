# # Use official Apache Spark image with Scala 2.12 support
# FROM apache/spark:3.5.1-scala

# # Switch to root to install packages
# USER root

# # Install Python3 and pip (if not already present)
# RUN apt-get update && \
#     apt-get install -y \
#         openjdk-11-jdk \
#         python3 \
#         python3-pip \
#         curl \
#         wget && \
#     rm -rf /var/lib/apt/lists/*

# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# ENV PATH=$PATH:$JAVA_HOME/bin:/opt/spark/bin
# ENV PYTHONPATH=/app

# # Set working directory
# WORKDIR /app

# # Copy connector source code
# COPY . /app

# # Install Python dependencies (data + Kafka + MinIO)
# RUN pip3 install --no-cache-dir \
#     pandas==2.0.2 \
#     pyarrow==16.1.0 \
#     minio==7.2.7 \
#     kafka-python==2.0.2 \
#     confluent-kafka==2.4.0 \
#     pyspark==3.5.1

# RUN apt-get update && apt-get install -y openjdk-11-jdk

# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# ENV PATH=$PATH:$JAVA_HOME/bin
# # Copy JDBC JARs for Postgres & MySQL into Spark classpath
# RUN mkdir -p /opt/jars
# RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
# #COPY spark-jars/postgresql-42.7.4.jar /opt/spark/jars/postgresql-42.7.4.jar
# # COPY ../spark-jars/mysql-connector-j-8.0.33.jar /opt/spark/jars/

# # -------------------------------------------------------
# # Install Iceberg JARs for Spark 3.5
# # -------------------------------------------------------
# ENV ICEBERG_VERSION=1.5.2

# RUN curl -fsSL \
#    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
#    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

# # -------------------------------------------------------
# # Install Hadoop AWS + AWS SDK bundle (REQUIRED FOR MINIO)
# # -------------------------------------------------------

# # Hadoop AWS
# RUN curl -fsSL \
#   -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
#      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# # Hadoop Client API
# RUN curl -fsSL \
#   -o /opt/spark/jars/hadoop-client-api-3.3.4.jar \
#      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar

# # Hadoop Client Runtime
# RUN curl -fsSL \
#   -o /opt/spark/jars/hadoop-client-runtime-3.3.4.jar \
#      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar

# # AWS SDK bundle (required for S3A + MinIO)
# RUN curl -fsSL \
#   -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
#      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# # Set environment variables
# ENV JAVA_HOME=/opt/java/openjdk \
#     SPARK_HOME=/opt/spark \
#     PATH=$PATH:/opt/java/openjdk/bin:/opt/spark/bin \
#     PYTHONPATH=/app

# # Default user (non-root for safety)
# USER 185

# # Default command
# CMD ["python3", "main.py"]

# Spark 3.5.1 + Scala 2.12 + Java 11 base
FROM apache/spark:3.5.1-python3

# Work as root to install packages and copy jars
USER root

# System packages and Python
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip curl wget ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir pyspark==3.5.1
# Environment: keep a single, valid JAVA_HOME and SPARK_HOME
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/java/openjdk/bin:/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    PYTHONPATH=/app

# App directory
WORKDIR /app

# Copy application code
COPY . /app

# Python deps (no pyspark here; it's provided by the base image)
RUN pip3 install --no-cache-dir \
    pandas==2.0.2 \
    pyarrow==16.1.0 \
    minio==7.2.7 \
    kafka-python==2.0.2 \
    confluent-kafka==2.4.0

# Add JDBC/Hadoop AWS/Iceberg jars into Spark classpath
# Postgres JDBC
RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.4.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
# Hadoop AWS + client libs
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/hadoop-client-api-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/hadoop-client-runtime-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
# Iceberg runtime for Spark 3.5 (Scala 2.12)
ENV ICEBERG_VERSION=1.5.2
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

# Drop privileges (Spark base image includes non-root user 185)
USER 185
ENTRYPOINT ["tail", "-f", "/dev/null"]
#ENTRYPOINT ["python3"]

# Default command for connector container
#CMD ["python3", "main.py"]

