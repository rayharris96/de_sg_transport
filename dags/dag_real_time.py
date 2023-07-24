from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from src.extract import call_lta_bus_api
from src.transform import transform_lta_bus
from src.load_combine import combine_bus_timing

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
    'transport_real_time',
    default_args=default_args,
    description='A DAG for simple bus and taxi timing and info ETL, in real-time mode',
    schedule_interval=timedelta(seconds=30),
)

# Define the PythonOperator
extract_bus_api = PythonOperator(
    task_id='call_lta_bus_api',
    python_callable=call_lta_bus_api,
    dag=dag,
)

transform_bus_data = PythonOperator(
    task_id='transform_bus_data',
    python_callable=transform_lta_bus,
    dag=dag,
)

combine_bus_data = PythonOperator(
    task_id='combine_bus_data',
    python_callable=combine_bus_timing,
    dag=dag,
)

start = DummyOperator(
    task_id='start_bus_timing_job',
    dag=dag,
)

end = DummyOperator(
    task_id='finish_bus_timing_job',
    dag=dag,
)

email = DummyOperator(
        task_id='send_email_alert',
        dag=dag,
)

start >> extract_bus_api >> transform_bus_data
transform_bus_data>> combine_bus_data >> end
transform_bus_data >> email
