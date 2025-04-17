from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def test_spark_connection():
    from pyspark.sql import SparkSession
    import socket
    
    # Afficher le nom d'hôte actuel pour le debug
    hostname = socket.gethostname()
    print(f"Nom d'hôte du conteneur: {hostname}")
    
    # Créer une session Spark locale (pas de connexion au cluster)
    spark = SparkSession.builder \
        .appName("LocalTest") \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"Version de Spark: {spark.version}")
    
    # Créer un DataFrame simple
    df = spark.createDataFrame([("Test",)], ["col1"])
    df.show()
    
    # Fermer la session
    spark.stop()
    
    return "Test Spark réussi"

dag = DAG(
    'spark_local_test',
    default_args=default_args,
    description='Test local de Spark dans Airflow',
    start_date=datetime(2025,4,8),
    catchup=False,
    tags=['test', 'spark']
)

test_task = PythonOperator(
    task_id='test_spark_local',
    python_callable=test_spark_connection,
    dag=dag,
)