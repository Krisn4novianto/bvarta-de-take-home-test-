import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date, try_to_timestamp, try_to_date


def get_spark(app_name="bronze_to_silver"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_parquet(spark, path):
    return spark.read.parquet(path)


def transform_events(df):
    return (
        df.withColumn("event_type", upper(trim(col("event_type"))))
        .withColumn("event_ts", try_to_timestamp(col("event_ts")))
        .withColumn("value", col("value").cast("double"))
        .withColumn("event_date", to_date("event_ts"))
    )


def transform_users(df):
    return df.withColumn("signup_date", try_to_date(col("signup_date"), "yyyy-MM-dd"))


def clean_events(df):
    clean = df.filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("event_ts").isNotNull()
    ).dropDuplicates(["event_id"])

    rejected = df.filter(
        col("event_id").isNull() |
        col("user_id").isNull() |
        col("event_ts").isNull()
    )
    return clean, rejected


def clean_users(df):
    return df.filter(col("user_id").isNotNull()).dropDuplicates(["user_id"])


def write_parquet(df, path, partition_col=None):
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)


def main(config_path):
    config = yaml.safe_load(open(config_path))
    cfg = config["pipeline"]["bronze_to_silver"]

    spark = get_spark()

    events = read_parquet(spark, cfg["bronze_events_path"])
    users = read_parquet(spark, cfg["bronze_users_path"])

    events = transform_events(events)
    users = transform_users(users)

    clean_ev, reject_ev = clean_events(events)
    clean_us = clean_users(users)

    write_parquet(clean_ev, cfg["silver_clean_events_path"], "event_date")
    write_parquet(clean_us, cfg["silver_clean_users_path"])
    write_parquet(reject_ev, cfg["silver_rejected_events_path"])

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    main(args.config)


