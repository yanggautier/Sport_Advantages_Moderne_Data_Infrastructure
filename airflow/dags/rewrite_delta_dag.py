import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Paramètre par défault pour le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'join_tables',
    default_args=default_args,
    description='Lit les données d\'activité sportive depuis Delta Lake',
    start_date=datetime(2025,4,8),
    catchup=False,
    max_active_runs=1,
    tags=['delta table', 'spark', 'activité sportive']
)

# Utiliser SparkSubmitOperator pour soumettre un job Spark qui lit les données Delta
read_delta_task = SparkSubmitOperator(
    task_id='read_delta_data',
    application='/opt/airflow/config/read_delta.py',
    conn_id='spark_default',
    application_args=[
        '--input_bucket', 'delta-tables',
        '--table', 'sport_activities',
        '--output_bucket', 'final-tables'
    ],
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': os.environ.get('MINIO_ROOT_USER'),
        'spark.hadoop.fs.s3a.secret.key': os.environ.get('MINIO_ROOT_PASSWORD'),
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.jars.packages': 'io.delta:delta-core_2.12:1.2.0'
    },
    jars="/opt/airflow/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/airflow/jars/hadoop-aws-3.3.1.jar,/opt/airflow/jars/delta-core_2.12-1.2.0.jar,/opt/airflow/jars/postgresql-42.5.1.jar",
    # exclude_packages='com.amazonaws:aws-java-sdk-bundle',
    verbose=True,
    dag=dag,
)


# Définir la tâche
read_delta_task