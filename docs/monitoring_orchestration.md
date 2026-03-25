# 🚀 Monitoring & Orchestration

## Overview

This section explains how the ETL pipeline is **monitored and orchestrated** using logging and Apache Airflow.

The pipeline follows a **medallion architecture**:

```
RAW → BRONZE → SILVER → GOLD
```

Each layer represents a stage of data processing with increasing levels of refinement and business value.

---

## 📊 Monitoring

Basic monitoring is implemented using **Python logging** inside each ETL script.

### What is monitored?

* ✅ Pipeline start and completion time
* ✅ Number of processed records
* ✅ Number of rejected/invalid records
* ✅ Error messages during transformation

### Example Logging Output

```
🚀 Starting Bronze Layer Processing
✅ Records processed: 10,000
⚠️ Records rejected: 120
🎯 Completed successfully
```

### Production Recommendation

For production environments, consider integrating:

* **Prometheus** → metrics collection
* **Grafana** → visualization dashboard
* **ELK Stack (Elasticsearch, Logstash, Kibana)** → centralized logging

---

## ⚙️ Orchestration with Apache Airflow

The pipeline is orchestrated using **Apache Airflow**, which provides:

* Scheduling (cron-based / daily jobs)
* Task dependency management
* Retry mechanism on failure
* Monitoring via UI
* Logging per task

---

## 🧩 Airflow DAG Pipeline

The repository includes a DAG:

```
data_pipeline_spark
```

### Pipeline Flow

```
raw_to_bronze
      ↓
bronze_to_silver
      ↓
silver_to_gold
```

Each stage is implemented as a **PythonOperator** that triggers a Spark job.

---

## 📁 Project Structure (Airflow)

```
airflow/
├── dags/
│   └── pipeline_dag.py     # Main DAG definition
├── logs/                   # Task logs (auto-generated)
├── plugins/                # Custom plugins (optional)
└── airflow.db              # Metadata database (auto-generated)
```

⚠️ **Important:**
Airflow only reads DAGs from:

```
$AIRFLOW_HOME/dags/
```

---

## 🛠️ Local Setup (WSL + Virtual Environment)

---

### 1. Prerequisites

* WSL2
* Ubuntu (WSL)
* Python 3.10+
* pip

Install WSL:

```bash
wsl --install
```

---

### 2. Clone Repository

```bash
git clone https://github.com/<your-repo>.git
cd bvarta-de-take-home-test-
```

If using Windows path:

```bash
cd /mnt/c/Users/User/Downloads/bvarta-de-take-home-test-
```

---

### 3. Install Python (if needed)

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv -y
```

---

### 4. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

---

### 5. Install Apache Airflow

```bash
pip install "apache-airflow==2.9.3" \
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-$(python3 --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2).txt"
```

---

### 6. Configure Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

---

### 7. Initialize Database

```bash
airflow db migrate
```

---

### 8. Move DAG File (PENTING BANGET 🔥)

Kalau DAG lo belum muncul, biasanya karena file belum di folder yang benar.

```bash
mv /mnt/c/Users/User/Downloads/bvarta-de-take-home-test/airflow/dags/pipeline_dag.py airflow/dags/
```

Cek:

```bash
ls airflow/dags
```

Harus muncul:

```
pipeline_dag.py
```

---

### 9. Start Airflow

```bash
airflow standalone
```

---

## 🌐 Access Airflow UI (WSL)

### 1. Get WSL IP

```bash
hostname -I
```

Example:

```
172.26.181.10
```

---

### 2. Open Browser

```
http://172.26.181.10:8080
```

---

### Login

```
Username: admin
Password: (auto-generated di terminal)
```

---

## ▶️ Running the Pipeline

### 1. Enable DAG

* Go to **DAGs**
* Find:

```
data_pipeline_spark
```

* Toggle → **ON**

---

### 2. Trigger DAG

Klik:

```
Trigger DAG
```

---

### 3. Monitor Execution

Gunakan:

* **Graph View** → lihat dependency
* **Grid View** → lihat status per run

---

## 📈 Monitoring di Airflow

Di UI Airflow lo bisa:

* ✅ Lihat status (success / failed)
* ✅ Lihat log tiap task
* ✅ Retry task yang gagal
* ✅ Monitor durasi eksekusi

---

## 🚨 Troubleshooting (IMPORTANT)

### ❌ DAG Tidak Muncul

**Fix:**

```bash
mv pipeline_dag.py airflow/dags/
```

---

### ❌ Broken DAG (tutorial_objectstorage.py)

**Fix:**

```bash
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

---

### ❌ Port 8080 Already in Use

**Fix:**

```bash
pkill -f airflow
airflow standalone
```

---

### ❌ Kubernetes / virtualenv Error

Abaikan saja (itu dari example DAG).

---

### ❌ Tidak Bisa Akses localhost

Gunakan:

```
http://<WSL-IP>:8080
```