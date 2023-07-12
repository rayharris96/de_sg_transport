from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from src.extract import call_lta_api

# Specify the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['raymond_harris@ssg.gov.sg'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 7, 10),
}

# Define the DAG
dag = DAG(
    'lta_api_dag',
    default_args=default_args,
    description='A DAG for simple bus timing and info ETL',
    schedule_interval=timedelta(days=1),
)

# Define the PythonOperator
call_api = PythonOperator(
    task_id='call_api',
    python_callable=call_lta_api,
    dag=dag,
)

dummy = DummyOperator(
    task_id='dummy_task',
    dag=dag)


if __name__ == "__main__":
    call_api >> dummy
