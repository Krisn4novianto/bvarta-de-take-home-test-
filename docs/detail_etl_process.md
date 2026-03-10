## Bronze Layer

The **Bronze layer** acts as the **landing zone** where raw data is ingested and stored in a more efficient storage format.

### Transformations

- Read JSON using **explicit schema**
- Convert data to **Parquet format**

### Output
`distribute/data/bronze/events`
`distribute/data/bronze/reference`


### Purpose

- Persist raw data
- Enable easy data reprocessing
- Improve storage and query performance

---

## Silver Layer

The **Silver layer** performs **data cleansing, validation, and standardization**.

### Transformations

#### 1. Standardization

| Column | Transformation |
|------|------|
| `event_type` | Converted to uppercase |
| whitespace | Removed using `trim()` |

#### 2. Type Casting

| Column | Type |
|------|------|
| `event_ts` | timestamp |
| `value` | double |

#### 3. Derived Columns

| Column | Description |
|------|------|
| `event_date` | Extracted from `event_ts` |

#### 4. Filtering Valid Records

A record is considered valid if:

- `event_id` is NOT NULL
- `user_id` is NOT NULL
- `event_ts` is valid

#### 5. Deduplication

- Duplicate events are removed using: (#dropDuplicates(["event_id"]))


### Outputs
Clean data:
`distribute/data/silver/clean/events`
`distribute/data/silver/clean/users`

Rejected records:
`distribute/data/silver/rejected/events`

Rejected records allow data engineers to investigate data quality issues.

---

## Enrichment Layer

Event data is enriched by joining it with user reference data.

### Join Operation

events LEFT JOIN users


### Additional Columns

| Column | Description |
|------|------|
| `is_purchase` | Flag if `event_type = PURCHASE` |
| `days_since_signup` | Difference between event date and signup date |

Using a **LEFT JOIN** ensures events remain available even if the user reference is missing.

---

## Gold Layer

The **Gold layer** contains **aggregated analytical metrics** used for reporting and dashboards.

### Aggregation Dimensions

- `event_date`
- `country`

### Generated Metrics

| Metric | Description |
|------|------|
| `total_events` | Total number of events |
| `total_value` | Total monetary value |
| `total_purchases` | Number of purchase events |
| `unique_users` | Number of unique users |

### Output Location
`distribute/data/gold/daily_metrics`

---

# Data Quality Rules

The pipeline applies several data quality checks.

### Required Fields

An event record is valid only if:
- event_id IS NOT NULL
- user_id IS NOT NULL
- event_ts IS NOT NULL

Invalid records are stored in: `silver/rejected/events`

---

### Data Standardization

- `event_type` converted to uppercase
- whitespace removed using `trim()`

### Duplicate Handling

Duplicate events are removed using: `dropDuplicates(["event_id"]`


### Type Validation

| Column | Validation |
|------|------|
| `event_ts` | parsed using `try_to_timestamp` |
| `value` | cast to `double` |

Records that fail parsing are considered invalid.

---

# Incremental & Late Data Strategy

The pipeline supports **incremental processing** based on `event_date`.

Data is stored using: `partitionBy("event_date")`


### Benefits

- Faster queries
- Efficient incremental processing
- Easier partition management

### Date Parameter

The pipeline supports an optional parameter: `--date`

**Terminal Script:** `python distribute/job/pipeline.py --config distribute/config/pipeline.yaml --date 2025-01-02`


If provided, the pipeline will display aggregated metrics only for the specified date.

---

# Handling Late Arriving Data

Late events may arrive after their original event date.

Strategy used:

- Data is written to the appropriate `event_date` partition
- Partitions can be safely updated using **dynamic partition overwrite**

Spark configuration: (#spark.sql.sources.partitionOverwriteMode = dynamic)


---

# Production Improvements

The following improvements could be implemented in a production environment.

### Schema Registry

Ensure schema consistency using tools such as:

- Confluent Schema Registry
- AWS Glue Schema Registry

### Data Quality Framework

Advanced validation using:

- Great Expectations
- Amazon Deequ

### Monitoring & Orchestration

Pipeline orchestration and monitoring using:

- Apache Airflow
- Prometheus
- Datadog

### Data Lineage

Track upstream and downstream dependencies using:

- OpenLineage
- DataHub

### Advanced Incremental Processing

Improve late data handling by reprocessing the last **N days** of partitions.
