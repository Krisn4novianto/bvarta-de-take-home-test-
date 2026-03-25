from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "Krisna Novianto",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="data_pipeline_spark",
    default_args=default_args,
    description="Spark Pipeline: Raw → Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    catchup=False,
    tags=["etl", "spark"],
)

SPARK = "/opt/spark/bin/spark-submit"
BASE_DIR = "/opt/airflow"


# ================= TASK FUNCTIONS =================

def raw_to_bronze():
    os.system(
        f"{SPARK} {BASE_DIR}/distribute/job/raw_to_bronze.py "
        f"--config {BASE_DIR}/distribute/config/pipeline.yaml"
    )


def bronze_to_silver():
    os.system(
        f"{SPARK} {BASE_DIR}/distribute/job/bronze_to_silver.py "
        f"--config {BASE_DIR}/distribute/config/pipeline.yaml"
    )


def silver_to_gold():
    os.system(
        f"{SPARK} {BASE_DIR}/distribute/job/silver_to_gold.py"
    )


# ================= OPERATORS =================

t1 = PythonOperator(
    task_id="raw_to_bronze",
    python_callable=raw_to_bronze,
    dag=dag,
)

t2 = PythonOperator(
    task_id="bronze_to_silver",
    python_callable=bronze_to_silver,
    dag=dag,
)

t3 = PythonOperator(
    task_id="silver_to_gold",
    python_callable=silver_to_gold,
    dag=dag,
)


# ================= DEPENDENCIES =================

t1 >> t2 >> t3

