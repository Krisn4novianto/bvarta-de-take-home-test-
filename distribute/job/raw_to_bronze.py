import argparse
import yaml
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# Load config
def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# Spark session
def get_spark(app_name: str = "raw-to-bronze") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_raw_events(spark: SparkSession, path: str):
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("value", StringType(), True)
    ])
    logger.info(f"Reading raw events from {path}")
    return spark.read.schema(schema).json(path)


def write_parquet(df, path: str, mode="append"):
    logger.info(f"Writing data to {path} with mode={mode}")
    df.write.mode(mode).parquet(path)


def read_users_reference(spark: SparkSession, path: str):
    logger.info(f"Reading users reference from {path}")
    return spark.read.csv(path, header=True, inferSchema=True)


def preview(df, name: str, n: int = 20):
    print(f"\n===== {name.upper()} PREVIEW =====\n")
    df.show(n, truncate=False)
    print(f"\n{'='*len(name)*2}\n")


def main(config_path: str):
    logger.info("Starting RAW → BRONZE ingestion")
    config = load_config(config_path)
    spark = get_spark()

    # Raw Events
    events_raw = read_raw_events(spark, config.get("raw_events_path", "distribute/data/raw/events"))
    write_parquet(events_raw, config.get("bronze_events_path", "distribute/data/bronze/events"), mode="append")
    preview(events_raw, "Raw Events")

    # Users Reference
    users = read_users_reference(spark, config.get("users_reference_path", "distribute/data/reference/users.csv"))
    write_parquet(users, config.get("bronze_reference_path", "distribute/data/bronze/reference"), mode="overwrite")
    preview(users, "Users Reference")

    spark.stop()
    logger.info("RAW → BRONZE pipeline finished successfully")


# CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()
    main(args.config)