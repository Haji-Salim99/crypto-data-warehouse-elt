from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner" : "haji",
    "retries": 1,
}

with DAG(
    dag_id="crypto_elt_dag",
    default_args=default_args,
    description="ELT pipeline for crypto data",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "elt", "data_engineering"]
) as dag:
    create_tables = BashOperator(
        task_id="create_tables",
        bash_command="python /app/scripts/create_tables.py"
    )

    load_raw = BashOperator(
        task_id="load_raw_data",
        bash_command="python /app/scripts/load_raw_to_postgres.py"
    )

    load_dimensions = BashOperator(
        task_id="load_dimensions",
        bash_command="python /app/scripts/load_dimensions.py"
    )

    load_fact = BashOperator(
        task_id="load_fact_table",
        bash_command="python /app/scripts/load_fact.py"
    )

    create_tables >> load_raw >> load_dimensions >> load_fact
