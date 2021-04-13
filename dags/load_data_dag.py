from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from include.load_database import load, get_latest_file
from plugins.python_api_operator import GenerateAPIOperator

default_args = {
    "owner": "mell",
    "depends_on_past": False,
    "email": ["mell@bank-banque-canada.ca"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
}


with DAG(
    "load_dataset_create_api",
    default_args=default_args,
    description="A demo pipeline that loads a file from the landing zone and automatically generates an API",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["example"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        poke_interval=5,
        fs_conn_id="landing_zone",
        filepath="*",
    )

    get_filename = PythonOperator(
        task_id='get_filename',
        python_callable=get_latest_file,
        op_kwargs={'filepath': '/opt/airflow/data'},
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load,
    )

    generate_api = GenerateAPIOperator(
        task_id="generate_api",
        dest='/opt/airflow/generated/api.py'
    )

    wait_for_file >> get_filename >> load_data >> generate_api

