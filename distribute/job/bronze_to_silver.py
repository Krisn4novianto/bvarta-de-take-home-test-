# distribute/job/bronze_to_silver_refactor.py
import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date, try_to_timestamp, try_to_date

# ================= SPARK SESSION =================
def get_spark(app_name: str = "bronze_to_silver") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

# ================= READ DATA =================
def read_parquet(spark: SparkSession, path: str):
    return spark.read.parquet(path)

# ================= TRANSFORM =================
def transform_events(df):
    return df \
        .withColumn("event_type", upper(trim(col("event_type")))) \
        .withColumn("event_ts", try_to_timestamp(col("event_ts"))) \
        .withColumn("value", col("value").cast("double")) \
        .withColumn("event_date", to_date("event_ts"))

def transform_users(df):
    return df.withColumn(
        "signup_date",
        try_to_date(col("signup_date"), "yyyy-MM-dd")
    )

# ================= CLEAN & REJECT =================
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
    clean = df.filter(col("user_id").isNotNull()).dropDuplicates(["user_id"])
    return clean

# ================= WRITE =================
def write_parquet(df, path: str, mode="overwrite", partition_col: str = None):
    writer = df.write.mode(mode)
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)

# ================= PREVIEW =================
def preview(df, name: str, n: int = 20):
    print(f"\n===== {name.upper()} =====")
    df.show(n, truncate=False)

# ================= MAIN =================
def main(config_path: str):
    with open(config_path) as f:
        config = yaml.safe_load(f)

    pipeline_cfg = config["pipeline"]["bronze_to_silver"]

    # Paths
    bronze_events_path = pipeline_cfg["bronze_events"]
    bronze_users_path = pipeline_cfg["bronze_users"]
    silver_clean_events = pipeline_cfg["silver_clean_events"]
    silver_clean_users = pipeline_cfg["silver_clean_users"]
    silver_rejected_events = pipeline_cfg["silver_rejected_events"]

    spark = get_spark()

    # Read Bronze
    events = read_parquet(spark, bronze_events_path)
    users = read_parquet(spark, bronze_users_path)

    # Transform
    events = transform_events(events)
    users = transform_users(users)

    # Clean & Reject
    clean_ev, rejected_ev = clean_events(events)
    clean_us = clean_users(users)

    # Write Silver
    write_parquet(clean_ev, silver_clean_events, partition_col="event_date")
    write_parquet(clean_us, silver_clean_users)
    write_parquet(rejected_ev, silver_rejected_events)

    # Preview
    preview(clean_ev, "Clean Events")
    preview(clean_us, "Clean Users")
    preview(rejected_ev, "Rejected Events")

    spark.stop()

# ================= CLI =================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()
    main(args.config)