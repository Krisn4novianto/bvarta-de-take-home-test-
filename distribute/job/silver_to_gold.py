import argparse
import yaml
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, count, sum, countDistinct
import psycopg2

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark():
    return SparkSession.builder.appName("silver-to-gold").getOrCreate()


def read_data(spark, events_path, users_path):
    return (
        spark.read.parquet(str(events_path)),
        spark.read.parquet(str(users_path))
    )


def enrich(events, users):
    return (
        events.join(users, "user_id", "left")
        .withColumn("is_purchase", when(col("event_type") == "PURCHASE", 1).otherwise(0))
        .withColumn("days_since_signup", datediff(col("event_date"), col("signup_date")))
    )


def aggregate(df):
    return df.groupBy("event_date", "country").agg(
        count("*").alias("total_events"),
        sum("value").alias("total_value"),
        sum("is_purchase").alias("total_purchases"),
        countDistinct("user_id").alias("unique_users")
    )


def write_parquet(df, path):
    df.write.mode("overwrite").partitionBy("event_date").parquet(str(path))


def load_postgres(df):
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

    df.write.mode("append").jdbc(
        url=jdbc_url,
        table="gold.daily_metrics",
        properties={
            "user": DB_USER,
            "password": DB_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
    )


def main():
    config_path = "/opt/airflow/distribute/config/pipeline.yaml"
    config = yaml.safe_load(open(config_path))
    cfg = config["pipeline"]["silver_to_gold"]

    spark = get_spark()

    BASE = Path(__file__).resolve().parent.parent

    events_path = BASE / cfg["silver_events_path"]
    users_path = BASE / cfg["silver_users_path"]
    gold_path = BASE / cfg["gold_metrics_path"]

    events, users = read_data(spark, events_path, users_path)

    df = enrich(events, users)
    gold = aggregate(df)

    write_parquet(gold, gold_path)
    load_postgres(gold)

    spark.stop()


if __name__ == "__main__":
    main()