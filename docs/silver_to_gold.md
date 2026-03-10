# Silver to Gold ETL Process

## Overview

The **Silver to Gold** pipeline transforms cleaned datasets from the **Silver layer** into aggregated analytics-ready datasets in the **Gold layer**.

At this stage, the pipeline performs several tasks:

- Data enrichment
- Feature generation
- Aggregation of business metrics
- Storage of analytics datasets in the Gold layer
- Monitoring using Prometheus metrics
- Data preview for validation

The transformation logic is implemented in:

```
distribute/job/silver_to_gold.py
```

---

# Input Data (Silver Layer)

The pipeline reads validated datasets from the **Silver storage layer**.

Location:

```
distribute/data/silver
```

Silver Structure:

```
silver
├── clean
│   ├── events
│   └── users
```

### Events Dataset

Path:

```
distribute/data/silver/clean/events
```

| Column | Description |
|------|-------------|
| event_id | Unique identifier of the event |
| user_id | Identifier of the user |
| event_type | Type of user activity |
| event_ts | Timestamp of the event |
| event_date | Partition column derived from timestamp |
| value | Numeric value associated with the event |

---

### Users Dataset

Path:

```
distribute/data/silver/clean/users
```

| Column | Description |
|------|-------------|
| user_id | Unique user identifier |
| country | Country of the user |
| signup_date | Date when the user registered |

---

# Transformation Process

The ETL job performs enrichment and aggregation to create analytics-ready datasets.

## 1. Data Enrichment

The pipeline joins the events dataset with the users dataset.

Join key:

```
user_id
```

Join type:

```
left join
```

This enrichment adds user attributes such as **country** and **signup_date** to the event data.

Additional derived columns are created:

### Purchase Indicator

```
is_purchase
```

Value:

- `1` if `event_type == "PURCHASE"`
- `0` otherwise

### Days Since Signup

```
days_since_signup
```

Calculated as:

```
datediff(event_date, signup_date)
```

This column measures the number of days between the user signup date and the event date.

---

## 2. Gold Aggregation

After enrichment, the dataset is aggregated to produce daily business metrics.

Aggregation keys:

```
event_date
country
```

Metrics generated:

| Metric | Description |
|------|-------------|
| total_events | Total number of events |
| total_value | Sum of event value |
| total_purchases | Number of purchase events |
| unique_users | Number of distinct users |

These metrics form the **daily analytics table** used for reporting and dashboards.

---

# Output Data (Gold Layer)

The aggregated dataset is written to the **Gold storage layer**.

Location:

```
distribute/data/gold
```

Gold Structure:

```
gold
└── daily_metrics
    └── event_date=YYYY-MM-DD
```

Dataset path:

```
distribute/data/gold/daily_metrics
```

Write configuration:

- Write mode: `overwrite`
- Partition column: `event_date`
- Format: `parquet`

Partitioning by event date allows efficient querying and incremental processing.

---

# Monitoring

The pipeline exposes metrics using **Prometheus**.

Metrics server:

```
http://localhost:8000
```

Metric collected:

```
events_processed_total
```

This counter tracks the total number of rows written to the Gold dataset.

---

# Data Preview

After writing the Gold dataset, the pipeline reads the Gold table and prints the results to the console.

Preview table:

```
gold/daily_metrics
```

If a date parameter is provided, the preview will filter the dataset using:

```
event_date == <date>
```

Otherwise, the full dataset is displayed.

---

# Running the Silver to Gold Pipeline

To execute the **Silver → Gold ETL process**, run the following command:

```bash
spark-submit distribute/job/silver_to_gold.py --date YYYY-MM-DD
```

The `--date` parameter is optional and can be used to filter the preview results.

---

# Integration with Airflow

This ETL stage is orchestrated by the Airflow DAG located at:

```
airflow/pipeline_dag.py
```

The DAG executes the pipeline stages in sequence:

1. Raw → Bronze  
2. Bronze → Silver  
3. **Silver → Gold**

---

# Final Output

The **Gold layer** contains aggregated datasets ready for:

- BI dashboards
- Business reporting
- Analytics queries
- Data warehouse exports