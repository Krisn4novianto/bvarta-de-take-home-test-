# Docker Setup Guide

## Overview

This project can be executed using **Docker** to simplify the environment setup.  
Docker allows the pipeline to run in an isolated container without manually installing Python, Spark, Java, and other dependencies on the host machine.

---

## 1. Check if Docker is Installed

Open a terminal (Command Prompt / PowerShell / Terminal) and run:

```bash
docker --version
````

If Docker is installed correctly, you should see output similar to:

```
Docker version 26.x.x, build xxxx
```

You can also verify Docker is running with:

```bash
docker info
```

---

## 2. Install Docker (If Not Installed)

If Docker is not installed, download **Docker Desktop** from the official website:

[https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)

Choose the installer based on your operating system:

* Windows
* macOS
* Linux

---

### Windows Installation Steps

1. Download **Docker Desktop for Windows**
2. Run the installer
3. Enable **WSL2 integration** if prompted
4. Restart your computer after installation
5. Launch **Docker Desktop**

---

## 3. Verify Docker Installation

After installation, restart your terminal and run:

```bash
docker --version
```

Example expected output:

```
Docker version 26.1.0
```

Then check that Docker is running:

```bash
docker ps
```

If Docker is working, it should display:

```
CONTAINER ID   IMAGE   COMMAND   CREATED   STATUS   PORTS   NAMES
```

---

# Run the Project Using Docker

Once Docker is installed and running, you can start the pipeline using the following steps.

---

## 1. Build the Docker Image

Navigate to the project directory:

```bash
cd bvarta-de-take-home-test
```

Build the Docker image:

```bash
docker build -t pyspark-event-pipeline .
```

This will create a Docker image named:

```
pyspark-event-pipeline
```

---

## 2. Run the Container

Start the container:

```bash
docker run -p 8080:8080 pyspark-event-pipeline
```

This command will start:

* Airflow scheduler
* Airflow webserver
* ETL orchestration pipeline

---

## 3. Access Airflow UI

Open your browser and go to:

```
http://localhost:8080
```

Default login credentials:

```
username: admin
password: admin
```

---

## 4. Run the Pipeline

Inside the Airflow UI:

1. Locate the DAG

```
data_pipeline
```

2. Enable the DAG
3. Click **Trigger DAG**

The pipeline will run in the following order:

```
raw_to_bronze
      ↓
bronze_to_silver
      ↓
silver_to_gold
```

You can monitor logs, retry tasks, and track execution directly from the Airflow interface.



