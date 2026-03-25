Oke, ini gue kasih versi **README yang udah bersih, profesional, dan siap dikirim ke reviewer/HR** — tanpa “curhat error”, tanpa noise 👇

---

# Silver to Gold ETL Process

## Overview

The **Silver to Gold** pipeline transforms cleaned datasets from the **Silver layer** into aggregated, analytics-ready datasets in the **Gold layer**.

This stage performs:

* Data enrichment (joining events and users)
* Feature engineering
* Aggregation of business metrics
* Writing results to the Gold layer (Parquet)
* Loading aggregated data into PostgreSQL (data warehouse)
* Monitoring via Prometheus
* Data preview for validation

Main script:

```
distribute/job/silver_to_gold.py
```

---

## Input Data (Silver Layer)

Location:

```
distribute/data/silver
```

Structure:

```
silver
├── clean
│   ├── events
│   └── users
```

---

### Events Dataset

Path:

```
distribute/data/silver/clean/events
```

| Column     | Description             |
| ---------- | ----------------------- |
| event_id   | Unique event identifier |
| user_id    | User identifier         |
| event_type | Type of activity        |
| event_ts   | Event timestamp         |
| event_date | Partition column        |
| value      | Numeric value           |

---

### Users Dataset

Path:

```
distribute/data/silver/clean/users
```

| Column      | Description            |
| ----------- | ---------------------- |
| user_id     | Unique user identifier |
| country     | User country           |
| signup_date | Registration date      |

---

## Transformation Process

### 1. Data Enrichment

The pipeline performs a left join:

```
events LEFT JOIN users ON user_id
```

Derived columns:

* **is_purchase**

  * 1 if event_type = "PURCHASE"
  * 0 otherwise

* **days_since_signup**

  * `datediff(event_date, signup_date)`

---

### 2. Aggregation

Data is grouped by:

```
event_date, country
```

Generated metrics:

| Metric          | Description               |
| --------------- | ------------------------- |
| total_events    | Total number of events    |
| total_value     | Sum of values             |
| total_purchases | Number of purchase events |
| unique_users    | Number of distinct users  |

---

## Output Data (Gold Layer)

Location:

```
distribute/data/gold/daily_metrics
```

Structure:

```
gold
└── daily_metrics
    └── event_date=YYYY-MM-DD
```

Configuration:

* Format: Parquet
* Mode: Overwrite
* Partitioned by: `event_date`

---

## PostgreSQL Integration

The aggregated dataset is also written to:

```
gold.daily_metrics
```

---

## Setup

### 1. Activate Virtual Environment

```bash
.\venv13\Scripts\Activate.ps1
```

---

### 2. Install Dependencies

```bash
pip install -r requirements.txt
pip install psycopg2-binary
```

---

### 3. Download PostgreSQL JDBC Driver

Download from:

[https://jdbc.postgresql.org/download/](https://jdbc.postgresql.org/download/)

Place the file:

```
jars/postgresql-42.7.3.jar
```

---

## Running the Pipeline

```bash
spark-submit --jars jars/postgresql-42.7.3.jar distribute/job/silver_to_gold.py --date 2025-01-01
```

Without date filter:

```bash
spark-submit --jars jars/postgresql-42.7.3.jar distribute/job/silver_to_gold.py
```

---

## Data Preview

After execution, the pipeline prints the aggregated dataset:

```
event_date | country | total_events | total_value | total_purchases | unique_users
```

If `--date` is provided, the output is filtered accordingly.


## Final Output

The Gold layer dataset is ready for:

* Business intelligence dashboards
* Reporting
* Analytical queries
* Data warehouse consumption