from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
}

with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    description="Run dbt models pipeline",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    dbt_run_all = BashOperator(
        task_id="dbt_run_all",
        bash_command="""
        docker exec \
        -e BQ_PROJECT_ID={{ var.value.BQ_PROJECT_ID }} \
        -e BQ_DATASET_NAME={{ var.value.BQ_DATASET_NAME }} \
        dbt dbt run --profiles-dir /usr/app/dbt --target dev
        """,
    )
