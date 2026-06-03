import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


AIRFLOW_HOME = Path("/opt/airflow")

if AIRFLOW_HOME not in sys.path:
    sys.path.append(str(AIRFLOW_HOME))

from scripts.gamma_ingest import run_gamma_ingestion
from scripts.beta_transform import run_beta_transform
from scripts.alpha_aggregate import run_alpha_aggregate
from scripts.snowflake_loading import run_snowflake_loading

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

    beta_transform_task = PythonOperator(
        task_id='beta_transform',
        python_callable=run_beta_transform,
        provide_context=True,
    )

    alpha_aggregate_task = PythonOperator(
        task_id='alpha_aggregate',
        python_callable=run_alpha_aggregate,
        provide_context=True,
    )

    load_snowflake_task = PythonOperator(
        task_id='snowflake_loading',
        python_callable=run_snowflake_loading,
        provide_context=True,
    )

    gamma_ingest_task >> beta_transform_task >> alpha_aggregate_task >> load_snowflake_task
