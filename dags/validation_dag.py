from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import great_expectations as ge
# Define the DAG and set the schedule to run daily
default_args = {
	  'owner': 'airflow',
	  'start_date': datetime(2024, 1, 1),
	  'retries': 1,
}
dag = DAG(
      'great_expectations_validation',
	default_args=default_args,
	schedule_interval='@daily',  # Runs once a day
)
# Define the function to run the validation
def run_validation():
    context = ge.data_context.DataContext()
    batch = context.get_batch(batch_kwargs, suite_name="your_expectation_suite")
    results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch])
    return results
# Set up the task in Airflow
validation_task = PythonOperator(
      task_id='run_great_expectations_validation',
      python_callable=run_validation,<
      dag=dag,
)
# Set the task in the DAG
validation_task