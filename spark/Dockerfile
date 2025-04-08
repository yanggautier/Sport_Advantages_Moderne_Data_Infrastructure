FROM bitnami/spark:3.2.3

USER root
RUN apt-get update && apt-get install -y curl dos2unix && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# Clean the script file to ensure proper encoding
RUN dos2unix /app/SaveDelta.scala && \
    sed -i '1s/^\xEF\xBB\xBF//' /app/SaveDelta.scala

CMD spark-shell --packages "io.delta:delta-core_2.12:2.0.2,io.delta:delta-storage:2.0.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3,org.apache.kafka:kafka-clients:3.1.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-s3:1.12.262" \
--exclude-packages "com.amazonaws:aws-java-sdk-bundle" \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--conf "spark.hadoop.fs.s3a.endpoint=http://${MINIO_ENDPOINT:-minio:9000}" \
--conf "spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER:-minio_user}" \
--conf "spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD:-minio_password}" \
--conf "spark.hadoop.fs.s3a.path.style.access=true" \
--conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
--conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
--conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" \
--conf "spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8" \
-i "/app/SaveDelta.scala"