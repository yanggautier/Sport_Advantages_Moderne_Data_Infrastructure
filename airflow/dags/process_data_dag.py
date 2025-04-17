import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Importer la fonction de traitement
from process_without_spark import process_data

# Paramètres par défaut pour le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Définir le DAG
dag = DAG(
    'process_data_without_spark',
    default_args=default_args,
    description='Traite les données sans utiliser Spark',
    start_date=datetime(2025, 4, 8),
    catchup=False,
    max_active_runs=1,
    tags=['python', 'activité sportive']
)

# Définir une fonction wrapper pour le PythonOperator
def process_data_task(**kwargs):
    input_bucket = kwargs.get('input_bucket', 'delta-tables')
    table = kwargs.get('table', 'sport_activities')
    output_bucket = kwargs.get('output_bucket', 'final-tables')
    
    return process_data(input_bucket, table, output_bucket)

# Créer la tâche de traitement des données
process_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data_task,
    op_kwargs={
        'input_bucket': 'delta-tables',
        'table': 'sport_activities',
        'output_bucket': 'final-tables'
    },
    dag=dag,
)

# Définir les dépendances des tâches (ici une seule tâche)
process_task