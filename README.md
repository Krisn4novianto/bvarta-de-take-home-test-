# Pyspark Event Data Pipeline

---

## Overview

A data engineering pipeline built with **PySpark** implementing a **Medallion Architecture (Raw → Bronze → Silver → Gold)** to process event tracking data and generate aggregated analytical metrics.

The pipeline ingests raw event data and user reference data, performs data cleansing and enrichment, and produces aggregated metrics suitable for analytics and dashboards.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Pipeline Architecture](#pipeline-architecture)
- [Data Sources](#data-sources)
- [ETL Process Details](#etl-process-details)
- [Local Setup (Manual Environment)](#local-setup-manual-environment)
- [ETL Pipeline](#etl-pipeline)
- [Run the ETL Pipeline (Local Setup)](#run-the-etl-pipeline-local-setup)
- [Run the Pipeline](#run-the-pipeline)
- [Monitoring & Orchestration](#monitoring--orchestration)
- [Run with Docker (Containerized Environment)](#run-with-docker-containerized-environment)

---

## Project Structure

```
├── airflow                             		# Contains Airflow DAGs used to orchestrate and schedule the ETL pipeline
│   └── pipeline_dag.py                 		# Main Airflow DAG responsible for orchestrating and scheduling the ETL workflow
│		
├── distribute                          		# Contains ETL pipeline scripts, configurations, and data storage
│   ├── config                          		# Configuration files used by the ETL pipeline
│   │   └── config.yaml                 		# Defines input/output paths, pipeline parameters, and environment settings
│   │		
│   ├── data                            		# Data lake storage organized using the Bronze–Silver–Gold architecture
│   │   ├── bronze                      		# Raw ingested data with minimal transformation
│   │   │   ├── events                  		# Raw event datasets ingested from source systems
│   │   │   └── reference               		# Reference datasets used for enrichment, validation, or lookup tables
│   │   │		
│   │   ├── silver                      		# Cleaned, validated, and structured datasets
│   │   │   ├── clean                   		# Valid datasets ready for downstream processing
│   │   │   │   ├── events              		# Cleaned events dataset
│   │   │   │   │   └── event_date=yyyy-mm-dd   # Partitioned events data by event date
│   │   │   │   │
│   │   │   │   └── users               		# Cleaned users dataset after validation and transformation
│   │   │   │		
│   │   │   └── rejected                		# Stores invalid records that failed validation rules
│   │   │       ├── events              		# Rejected event records
│   │   │       └── users               		# Rejected user records
│   │   │		
│   │   └── gold                        		# Aggregated and analytics-ready datasets
│   │       └── daily_metrics           		# Daily aggregated metrics generated from Silver datasets
│   │           └── event_date=yyyy-mm-dd   	# Partitioned aggregated metrics by event date
│   │		
│   └── job                             		# Contains ETL processing scripts for each pipeline stage
│       ├── raw_to_bronze.py            		# Ingest raw data from source systems into the Bronze layer
│       ├── bronze_to_silver.py         		# Clean, validate, and transform Bronze data into structured Silver datasets
│       └── silver_to_gold.py           		# Aggregate and transform Silver datasets into analytics-ready Gold datasets
│		
├── docs                                		# Project documentation related to the ETL pipeline
│   ├── bronze_to_silver.md             		# Documentation for data cleaning and validation processes
│   ├── detail_data_source.md           		# Description of all data sources used in the pipeline
│   ├── detail_etl_process.md           		# End-to-end explanation of the ETL workflow
│   ├── docker_setup.md                 		# Instructions for building and running the project using Docker
│   ├── monitoring_orchestration.md     		# Documentation for monitoring and orchestration setup (Airflow, Prometheus)
│   ├── raw_to_bronze.md                		# Documentation for the raw data ingestion process
│   └── silver_to_gold.md               		# Documentation for Silver-to-Gold aggregation and transformation
│		
├── README.md                           		# Main project documentation including setup instructions and pipeline overview
├── requirements.txt                    		# Python dependencies required to run the ETL pipeline
├── dockerfile                          		# Configuration file used to build the Docker image for the pipeline environment
├── .gitignore                          		# Specifies files and directories that Git should ignore
└── .dockerignore                       		# Specifies files and directories excluded during Docker image build
```

---

## Pipeline Architecture

The pipeline follows the **Medallion Architecture** pattern. Each layer has a specific responsibility:

| Layer  | Purpose                                         |
|--------|-------------------------------------------------|
| Raw    | Store source data exactly as received          |
| Bronze | Store ingested raw data in optimized format   |
| Silver | Perform data cleaning and validation          |
| Gold   | Generate aggregated metrics for analytics     |

---

## Data Sources

The pipeline processes data from multiple sources.  
For detailed information:  

➡️ [Detail Data Source](docs/detail_data_source.md)

---

## ETL Process Details

The ETL pipeline extracts raw data, performs validation and transformation, and loads the results into the target layers (Bronze, Silver, Gold).  

Detailed explanation of each stage: extraction, validation, transformation, and loading.  

➡️ [View Detailed ETL Process](docs/detail_etl_process.md)

---



## Local Setup (Manual Environment)

If you prefer to run the pipeline on your local machine without Docker, you first need to set up the required environment including Java, Spark, and Python.

---

### 1. Install Java 17

Download **Java 17 (Temurin)** from:

[https://adoptium.net/](https://adoptium.net/)

Choose the installer based on your OS (Windows / macOS / Linux).

Verify installation:

```powershell
java -version

```


---
#### Step 2 — Install Java

Run the downloaded installer and follow the installation instructions.

Make sure Java is added to your **system PATH** during installation.

---
#### Step 3 — Verify Installation

After installation, open your terminal or command prompt and run:

```powershell
java -version
```

You should see output similar to: **openjdk version "17.0.18_8"**


### 2. Download Apache Spark

Download Spark 4.1.1 built with Hadoop 3:

```powershell
https://www.apache.org/dyn/closer.lua/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz
```
- Extract the **.tgz** file using 7-Zip:
- Right click → 7-Zip → Extract Here → Result `.tar` file
- Extract the **.tar**  file using 7-Zip to your final folder, Example destination:
   
  ```powershell
  C:\Users\User\Downloads\bvarta-de-take-home-test\spark-4.1.1-bin-hadoop3
  ```

### 3. Setup Python Environment

Create a Python virtual environment:

```powershell
py -3.13 -m venv venv13
.\venv13\Scripts\Activate.ps1
```

Verify Python version:
```powershell
python -V  
```
Expected version: **Python 3.13**

### 4. Install Dependencies

Install all required Python packages using the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

### 5. Configure Environment Variables

These environment variables allow PySpark to locate Spark, Hadoop, and the Python interpreter.

```powershell
$env:SPARK_HOME = "C:\Users\User\Downloads\bvarta-de-take-home-test\spark-4.1.1-bin-hadoop3"
$env:PATH = "$env:SPARK_HOME\bin;" + $env:PATH
$env:HADOOP_HOME="C:\Users\User\Downloads\bvarta-de-take-home-test\hadoop"
$env:PATH="$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON="$PWD\venv13\Scripts\python.exe"
```

### 6. Test PySpark

Run the PySpark shell to verify the setup:

```powershell
pyspark
```

If PySpark starts successfully, exit using:
```powershell
exit()
```

---

### 7. ETL Pipeline

This project implements a **multi-layer ETL pipeline** following the Medallion Architecture: 

`Raw → Bronze → Silver → Gold`
   
Each stage of the pipeline performs different transformations and validations on the data.


### **Raw to Bronze**

This step ingests **raw data** from the source system and stores it in the **Bronze layer**.
      
The Bronze layer keeps the data **as close as possible to the original source**, with minimal transformation.  
This layer is mainly used for **data traceability and recovery**.
      
📄 See detailed documentation: [Raw to Bronze Process](docs/raw_to_bronze.md)


###  **Bronze to Silver**

This stage performs **data cleaning, validation, and transformation**.

Invalid records are separated into a **rejected dataset**, while valid records are written into the **Silver layer**.

📄 See detailed documentation: [Bronze to Silver Process](docs/bronze_to_silver.md)



### **Silver to Gold**

The Gold layer contains **business-ready datasets** that are optimized for analytics, reporting, and downstream consumption.

Data in this layer is typically **aggregated, enriched, or modeled** for analytical use cases.

📄 See detailed documentation: [Silver to Gold Process](docs/silver_to_gold.md)

---

### 8. Pipeline With Date Filter (Optional)

You can run the pipeline for a specific date:

```powershell
python distribute/job/pipeline.py --config distribute/config/pipeline.yaml --date YYY-MM-DD  
```

If the `--date` parameter is provided, the pipeline will display the **Gold layer metrics** for the specified date only.


---

## Monitoring & Orchestration

The ETL pipeline is orchestrated using Apache Airflow.

Airflow provides:
- Pipeline scheduling
- Task dependency management
- Execution monitoring
- Automatic retries for failed tasks

➡️ Full orchestration documentation: [Monitoring & Orchestration Details](docs/monitoring_orchestration.md)


## Run with Docker (Containerized Environment)

The pipeline can also be executed inside a Docker container, which provides a reproducible environment with all dependencies pre-installed: Python, Java, Spark, and Airflow.

Running with Docker eliminates manual setup and ensures consistent behavior across systems.

For detailed instructions: **[Docker Setup Guide](docs/docker_setup.md)**