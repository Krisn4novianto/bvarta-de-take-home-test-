import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, count, sum, countDistinct
from prometheus_client import Counter, start_http_server

# ================= PROMETHEUS =================
start_http_server(8000)
events_processed = Counter(
    "events_processed_total",
    "Total events processed"
)

# ================= LOGGER =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ================= SPARK SESSION =================
def get_spark(app_name: str = "silver-to-gold-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

# ================= READ DATA =================
def read_silver_data(spark: SparkSession, events_path: str, users_path: str):
    logger.info(f"Reading silver events from {events_path}")
    events = spark.read.parquet(events_path)
    logger.info(f"Reading silver users from {users_path}")
    users = spark.read.parquet(users_path)
    return events, users

# ================= ENRICH =================
def enrich_events(events, users):
    logger.info("Starting enrichment")
    return events.join(users, "user_id", "left") \
        .withColumn("is_purchase", when(col("event_type") == "PURCHASE", 1).otherwise(0)) \
        .withColumn("days_since_signup", datediff(col("event_date"), col("signup_date")))

# ================= AGGREGATE =================
def aggregate_gold(enriched):
    logger.info("Starting gold aggregation")
    return enriched.groupBy("event_date", "country").agg(
        count("*").alias("total_events"),
        sum("value").alias("total_value"),
        sum("is_purchase").alias("total_purchases"),
        countDistinct("user_id").alias("unique_users")
    )

# ================= WRITE =================
def write_gold(df, path: str, partition_col: str = "event_date"):
    logger.info(f"Writing gold data to {path}")
    df.write.mode("overwrite").partitionBy(partition_col).parquet(path)
    processed = df.count()
    events_processed.inc(processed)
    logger.info(f"Gold rows written: {processed}")

# ================= PREVIEW =================
def preview_gold(spark: SparkSession, path: str, date_filter=None, n=20):
    df = spark.read.parquet(path)
    if date_filter:
        df = df.filter(col("event_date") == date_filter)
    print("\n===== GOLD DAILY METRICS =====\n")
    df.show(n, truncate=False)
    print("\n==============================\n")

# ================= MAIN =================
def main(date_filter=None):
    logger.info("Starting Silver → Gold Pipeline")
    spark = get_spark()

    # Paths
    events_path = "distribute/data/silver/clean/events"
    users_path = "distribute/data/silver/clean/users"
    gold_path = "distribute/data/gold/daily_metrics"

    # Pipeline
    events, users = read_silver_data(spark, events_path, users_path)
    enriched = enrich_events(events, users)
    gold = aggregate_gold(enriched)
    write_gold(gold, gold_path)
    preview_gold(spark, gold_path, date_filter=date_filter)

    spark.stop()
    logger.info("Silver → Gold Pipeline finished successfully")

# ================= CLI =================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=False, help="Filter gold preview by event_date")
    args = parser.parse_args()
    main(args.date)