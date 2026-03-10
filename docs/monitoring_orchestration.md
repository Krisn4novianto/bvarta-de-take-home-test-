# Monitoring & Orchestration

## Overview

This section explains how the ETL pipeline is monitored and orchestrated.

---

## Monitoring

Basic monitoring is implemented using **Python logging**, which allows tracking:

- Pipeline start and completion
- Number of processed records
- Rejected records

In production environments, additional monitoring tools such as **Prometheus** can be integrated to collect pipeline metrics and monitor system performance.

---

## Orchestration with Apache Airflow

The pipeline is orchestrated using **Apache Airflow**, which enables automated scheduling, monitoring, and retry mechanisms for the ETL pipeline.

The repository includes an Airflow DAG that orchestrates the entire pipeline from **RAW → BRONZE → SILVER → GOLD** in a single workflow.

Each stage of the pipeline is executed as a separate Airflow task, ensuring that downstream processes run only after upstream tasks complete successfully.

---

## Project Structure (Airflow)


```
airflow/                           # Contains Apache Airflow configuration and workflow definitions.
├── dags/                          # Directory where Airflow automatically discovers DAG files.
│   └── pipeline_dag.py            # Defines the full ETL pipeline (RAW → BRONZE → SILVER → GOLD).
└── .gitignore                     # Specifies files and folders that should not be tracked by Git.
```

Inside `pipeline_dag.py`, the pipeline tasks are executed sequentially:

```
raw_to_bronze
↓
bronze_to_silver
↓
silver_to_gold
```

This structure ensures that each stage runs only after the previous stage has successfully completed.

---

## Local Setup (Windows)

### 1. Install Apache Airflow

Install Airflow using pip:

```powershell
pip install apache-airflow
````

---

### 2. Navigate to the Project Directory

Change directory to your project location:

```powershell
cd <your-project-path>\bvarta-de-take-home-test
```

Example:

```powershell
cd C:\Users\User\Downloads\bvarta-de-take-home-test
```

---

### 3. Set Airflow Home Directory

Set the `AIRFLOW_HOME` environment variable so Airflow uses the project's Airflow folder.

```powershell
$env:AIRFLOW_HOME="<your-project-path>\bvarta-de-take-home-test\airflow"
```

Example:

```powershell
$env:AIRFLOW_HOME="C:\Users\User\Downloads\bvarta-de-take-home-test\airflow"
```

---

### 4. Start Airflow

Run the following command to start Airflow:

```powershell
airflow standalone
```

This command will start:

* Airflow scheduler
* Airflow webserver
* Airflow metadata database

---

### 5. Access the Airflow UI

Open the Airflow UI in your browser:

```
http://localhost:8080
```

Default credentials:

```
username: admin
password: admin
```

From the UI you can:

* Trigger pipeline runs
* Monitor execution status
* Inspect logs
* Retry failed tasks

---

### 6. Run the Pipeline

Inside the Airflow UI:

1. Locate the DAG:

```
data_pipeline
```

2. Enable the DAG.

3. Click **Trigger DAG**.

Airflow will execute the ETL pipeline in the following order:

```
raw_to_bronze
      ↓
bronze_to_silver
      ↓
silver_to_gold
```

From the Airflow UI you can:

* Monitor task execution status
* Inspect pipeline logs
* Retry failed tasks
* Track pipeline execution duration

