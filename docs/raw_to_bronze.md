# Raw to Bronze ETL Process

## Overview

The **Raw to Bronze** pipeline ingests raw data from source storage and writes it into the **Bronze layer** of the data lake.

At this stage, the pipeline performs several tasks:

- Raw data ingestion
- Schema enforcement for event data
- Reference data ingestion
- Storage of datasets into the Bronze layer
- Data preview for pipeline validation

The ingestion logic is implemented in:

```
distribute/job/raw_to_bronze.py
```

---

# Input Data (Raw Layer)

The pipeline reads raw datasets from the **Raw data layer**.

Location:

```
distribute/data/raw
```

Raw Structure:

```
raw
└── events
```

Reference data is also used in this stage.

Reference Location:

```
distribute/data/reference
```

Reference Structure:

```
reference
└── users.csv
```

---

# Ingestion Process

The ETL job ingests raw datasets and stores them in the Bronze layer.

## 1. Schema Definition

The pipeline defines a schema for raw event data to ensure consistent structure during ingestion.

Defined schema:

| Column | Type |
|------|------|
| event_id | string |
| user_id | string |
| event_type | string |
| event_ts | string |
| value | string |

This schema is applied when reading the raw events dataset.

---

## 2. Raw Events Ingestion

The pipeline reads event data from the raw storage directory.

```
distribute/data/raw/events
```

Spark reads the data using the defined schema and loads it into the `events_raw` dataset.

---

## 3. Write Events to Bronze

The ingested events dataset is written to the Bronze layer.

Output location:

```
distribute/data/bronze/events
```

Write configuration:

- Write mode :  `append`
- Format     :  `parquet`

This ensures that new incoming event files are continuously appended to the Bronze dataset.

---

## 4. Users Reference Ingestion

The pipeline also ingests a reference dataset containing user information.

Input file:

```
distribute/data/reference/users.csv
```

Spark reads the CSV file with:

- `header=True`
- `inferSchema=True`

---

## 5. Write Reference Data to Bronze

The users reference dataset is written to the Bronze layer.

Output location:

```
distribute/data/bronze/reference
```

Write Configuration:

- Write mode: `overwrite`
- Format: `parquet`

This ensures the reference dataset is refreshed during each pipeline run.

---

# Data Preview

To help validate the ingestion process, the pipeline prints previews of the datasets.

Preview includes:

- Raw events data
- Users reference data

These previews allow verification that the pipeline successfully read the input data.

---

# Output Data (Bronze Layer)

The ingested datasets are written to the **Bronze storage layer**.

Location:

```
distribute/data/bronze
```

Bronze Structure:

```
bronze
├── events
└── reference
```

### Events Dataset

Path:

```
bronze/events
```

Contains ingested raw event records stored in **Parquet format**.

### Reference Dataset

Path:

```
bronze/reference
```

Contains the users reference dataset converted from CSV into **Parquet format**.

---

# Running the Raw to Bronze Pipeline

To execute the **Raw → Bronze ETL process**, run the following command:

```powershell
spark-submit distribute/job/raw_to_bronze.py --config distribute/config/config.yaml
```

The configuration file defines pipeline settings.

Configuration location:

```
distribute/config/config.yaml
```

---

# Integration with Airflow

This ETL stage is orchestrated by the Airflow DAG located at:

```
airflow/pipeline_dag.py
```

The DAG executes the pipeline stages in sequence:

1. **Raw → Bronze**  
2. Bronze → Silver  
3. Silver → Gold  

---

# Next Step

After raw data is successfully ingested into the Bronze layer, the next stage performs data cleaning and validation.

See: [Bronze to Silver Process](bronze_to_silver.md)