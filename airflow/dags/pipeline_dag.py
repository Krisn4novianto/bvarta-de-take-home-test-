from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "Krisna Novianto",
    "start_date": datetime(2025, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="pipeline_dag",
    schedule="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    raw_to_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command="""
        python /mnt/c/Users/User/Downloads/bvarta-de-take-home-test/distribute/job/raw_to_bronze.py \
        --config /mnt/c/Users/User/Downloads/bvarta-de-take-home-test/distribute/config/pipeline.yaml
        """
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        python /mnt/c/Users/User/Downloads/bvarta-de-take-home-test/distribute/job/bronze_to_silver.py
        """
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
        python /mnt/c/Users/User/Downloads/bvarta-de-take-home-test/distribute/job/silver_to_gold.py
        """
    )

    raw_to_bronze >> bronze_to_silver >> silver_to_gold
