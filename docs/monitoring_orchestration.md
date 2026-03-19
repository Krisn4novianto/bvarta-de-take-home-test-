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

Panduan ini menjelaskan cara menjalankan **Airflow orchestration** untuk pipeline ini menggunakan **WSL2 + Python virtual environment**.

---

#### 1️⃣ Prerequisites

Pastikan sudah terinstall:

* **WSL2**
* **Ubuntu (WSL)**
* **Python 3.10+**
* **pip**

Install WSL jika belum ada:

```bash
wsl --install
```

Masuk ke Ubuntu:

```bash
wsl -d Ubuntu
```

---

#### 2️⃣ Clone Repository

Clone project ke komputer kamu.

```bash
git clone https://github.com/<your-repo>.git
```

Masuk ke folder project:

```bash
cd AstraWorld
```

Jika project berada di Windows path:

```bash
cd /mnt/c/Users/User/Downloads/AstraWorld
```

---

#### 3️⃣ Verify Python Installation

Pastikan Python sudah tersedia di WSL.

```bash
python3 --version
```

Minimal:

```
Python 3.10+
```

Jika belum ada:

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv -y
```

---

#### 4️⃣ Create Virtual Environment

Buat environment terpisah agar dependency project tidak bercampur dengan sistem.

```bash
python3 -m venv venv
```

Aktifkan virtual environment:

```bash
source venv/bin/activate
```

Jika ingin reset environment:

```bash
rm -rf venv
python3 -m venv venv
source venv/bin/activate
```

---

#### 5️⃣ Install Apache Airflow

Install Airflow dengan constraint yang sesuai dengan versi Python.

```bash
pip install "apache-airflow==2.9.3" \
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-$(python3 --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2).txt"
```

Verifikasi instalasi:

```bash
airflow version
```

---

#### 6️⃣ Configure AIRFLOW_HOME

Set folder Airflow agar berada di dalam project.

```bash
export AIRFLOW_HOME=$(pwd)/airflow
```

Struktur folder Airflow akan dibuat seperti ini:

```
bvarta-de-take-home-test-
│
├── airflow
│   ├── dags
│       ├── pipeline_dag.py
│   ├── logs
│   ├── plugins
│   └── airflow.db
```

Airflow akan membaca DAG dari folder:

```
$AIRFLOW_HOME/dags/
```

---

#### 7️⃣ Initialize Airflow Database

Jalankan perintah berikut untuk inisialisasi database metadata Airflow:

```bash
airflow db migrate
```

---

#### 8️⃣ Start Airflow

Jalankan Airflow dalam mode standalone:

```bash
airflow standalone
```

Perintah ini akan otomatis menjalankan:

* Webserver
* Scheduler
* Triggerer
* Inisialisasi database (jika belum)

> ⚠️ Setelah menjalankan perintah ini, **jangan tutup terminal** karena Airflow berjalan di sini.


---

### 5. Access the Airflow UI

### ⚠️ Penting

Pada environment **WSL (Windows Subsystem for Linux)**, **jangan gunakan**:

```
http://localhost:8080
```

---

## ✅ Langkah Akses Web UI

### 1. Buka terminal baru (WSL)

```bash
wsl -d Ubuntu
```

---

### 2. Masuk ke project

```bash
cd /mnt/c/Users/User/Downloads/AstraWorld
```

---

### 3. Ambil IP Address WSL

```bash
hostname -I
```

Contoh output:

```
172.26.181.10
```

---

### 4. Buka di browser

```
http://172.26.181.10:8080
```

---

## 🔐 Login Airflow

Saat menjalankan:

```bash
airflow standalone
```

Akan muncul informasi login seperti berikut:

```
Login with username: admin
password: xxxxxxxxx
```

Gunakan credential tersebut untuk login ke Airflow Web UI.

---

## 🌐 Airflow Web UI

Setelah berhasil login ke **Airflow Web UI**, langkah selanjutnya adalah **menjalankan dan memonitor pipeline**.

---

### 🔍 1. Masuk ke Menu DAGs

Pada halaman utama Airflow:

1. Klik menu **DAGs**
2. Akan muncul daftar pipeline yang tersedia
3. Cari DAG berikut:

```
daily_pipeline
```

---

### ▶️ 2. Aktifkan DAG

Sebelum menjalankan pipeline, DAG harus diaktifkan terlebih dahulu.

Langkah-langkah:

1. Klik **toggle switch** di sebelah kiri nama DAG
2. Pastikan status berubah menjadi **ON (aktif)**

⚠️ **Catatan:**
Jika DAG tidak diaktifkan, pipeline **tidak dapat dijalankan** baik secara manual maupun otomatis.


---

### 6. Run the Pipeline

Inside the Airflow UI:

1. Locate the DAG:

```
pipeline_dag.py
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

### 7. Monitoring Pipeline

Setelah pipeline dijalankan:

1. Klik nama DAG **`daily_pipeline`**
2. Buka salah satu tampilan berikut:

   * **Grid View**
   * **Graph View**

Di halaman ini Anda dapat melihat **status setiap task**.
