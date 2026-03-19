from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

# Set timezone Asia/Jakarta
local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "Krisna Novianto",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    "retries": 1
}

with DAG(
        dag_id="pipeline_dag",
        description="""
    End-to-end daily data pipeline orchestrating data ingestion, transformation, 
    and aggregation processes within the data warehouse. 
    """,
        schedule="@daily",
        catchup=False,
        default_args=default_args,
        tags=["data-engineering", "etl", "data-pipeline", "daily-job"]
) as dag:




    # -----------------------------
    # Task 1: RAW → BRONZE
    # -----------------------------
    raw_to_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command="""
        python /mnt/c/Users/User/Downloads/bvarta-de-take-home-test/distribute/job/raw_to_bronze.py \
        --config /mnt/c/Users/User/Downloads/bvarta-de-take-home-test/distribute/config/pipeline.yaml
        """
    )

    # -----------------------------
    # Task 2: BRONZE → SILVER
    # -----------------------------
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        python /mnt/c/Users/User/Downloads/bvarta-de-take-home-test/distribute/job/bronze_to_silver.py
        """
    )

    # -----------------------------
    # Task 3: SILVER → GOLD
    # -----------------------------
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
        python /mnt/c/Users/User/Downloads/bvarta-de-take-home-test/distribute/job/silver_to_gold.py
        """
    )

    # Workflow dependency
    raw_to_bronze >> bronze_to_silver >> silver_to_gold
