import argparse
import yaml
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_spark(app_name="raw-to-bronze"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_raw_events(spark, path):
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("value", StringType(), True),
    ])
    logger.info(f"Reading raw events from {path}")
    return spark.read.schema(schema).json(path)


def read_users_reference(spark, path):
    logger.info(f"Reading users reference from {path}")
    return spark.read.csv(path, header=True, inferSchema=True)


def write_parquet(df, path, mode="append"):
    logger.info(f"Writing to {path} mode={mode}")
    df.write.mode(mode).parquet(path)


def main(config_path: str):
    logger.info("Starting RAW → BRONZE")

    config = load_config(config_path)
    cfg = config["pipeline"]["raw_to_bronze"]

    spark = get_spark()

    events = read_raw_events(spark, cfg["raw_events_path"])
    write_parquet(events, cfg["bronze_events_path"], "append")

    users = read_users_reference(spark, cfg["users_reference_path"])
    write_parquet(users, cfg["bronze_reference_path"], "overwrite")

    spark.stop()
    logger.info("RAW → BRONZE DONE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    main(args.config)