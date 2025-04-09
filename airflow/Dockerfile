FROM apache/airflow:2.6.3

USER root

# Installer les dépendances système
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps wget unzip && \
    apt-get clean

# Télécharger et installer Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

USER airflow

# Configurer les variables d'environnement
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=python3

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt