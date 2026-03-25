FROM python:3.11-slim

# ================= SYSTEM DEPENDENCIES =================
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# ================= ENV =================
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV AIRFLOW_HOME=/opt/airflow
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

WORKDIR /opt/airflow

# ================= INSTALL SPARK =================
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark.tgz \
    && tar -xzf spark.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME \
    && rm spark.tgz
    
# ================= PYTHON DEPENDENCIES =================
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ================= COPY PROJECT =================
COPY . .

EXPOSE 8080

CMD ["bash"]