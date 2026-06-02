import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = Path("/opt/airflow")

if AIRFLOW_HOME not in sys.path:
    sys.path.append(str(AIRFLOW_HOME))

from scripts.gamma_ingest import run_gamma_ingestion

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='flight_ops_medallion_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['flight_ops', 'medallion'],
) as dag:

    gamma_ingest_task = PythonOperator(
        task_id='gamma_ingest',
        python_callable=run_gamma_ingestion,
        provide_context=True,
    )
