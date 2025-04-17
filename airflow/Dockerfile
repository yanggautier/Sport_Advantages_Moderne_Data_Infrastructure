FROM apache/airflow:2.6.3

USER root

# Installer les dépendances système
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps wget unzip && \
    apt-get clean

# Créer un répertoire pour les JARs
RUN mkdir -p /opt/airflow/jars

# Télécharger les JARs nécessaires
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar -O /opt/airflow/jars/aws-java-sdk-bundle-1.11.901.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -O /opt/airflow/jars/hadoop-aws-3.3.1.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.2.0/delta-core_2.12-1.2.0.jar -O /opt/airflow/jars/delta-core_2.12-1.2.0.jar && \
    # wget -q https://repo1.maven.org/maven2/io/trino/trino-delta-lake/413/trino-delta-lake-413.jar -O /opt/airflow/jars/trino-delta-lake-413.jar  && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar -O /opt/airflow/jars/postgresql-42.5.1.jar

# Définir les permissions appropriées
RUN chmod 644 /opt/airflow/jars/*.jar

# Télécharger et installer Spark
ENV SPARK_VERSION=3.2.3
ENV HADOOP_VERSION=2.7
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