from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'rishi',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'nasa_asteroid_medallion_pipeline',
    default_args=default_args,
    description='Automated Medallion Architecture Engine using Batch PySpark',
    schedule_interval='@daily',
    catchup=False
) as dag:

    ingest_to_bronze = BashOperator(
        task_id='ingest_to_bronze',
        bash_command='echo "Executing API Extraction Tasks..." && sleep 5',
    )

    transform_to_silver = BashOperator(
        task_id='transform_to_silver',
        bash_command='python3 /opt/airflow/scripts/worker.py silver --format parquet',
    )

    transform_to_gold = BashOperator(
        task_id='transform_to_gold',
        bash_command='python3 /opt/airflow/scripts/worker.py gold --format parquet',
    )

    # Topological execution workflow configuration
    ingest_to_bronze >> transform_to_silver >> transform_to_gold