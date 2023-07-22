from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from src.extract import call_erp_api
from src.transform import transform_lta_erp_rate

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
    'transport_batch',
    default_args=default_args,
    description='A DAG for erp rate and info ETL, in batch mode',
    schedule_interval=timedelta(days=1),
)

# Define the PythonOperator
call_api = PythonOperator(
    task_id='call_erp_api',
    python_callable=call_erp_api,
    dag=dag,
)


transform_erp_api = PythonOperator(
    task_id='transform_erp_data',
    python_callable=transform_lta_erp_rate,
    dag=dag,
)


call_api >> transform_erp_api

