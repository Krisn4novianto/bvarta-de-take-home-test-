FROM python:3.13-slim

RUN apt-get update && \
    apt-get install -y default-jdk curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV AIRFLOW_HOME=/app/airflow

EXPOSE 8080

CMD ["airflow", "standalone"]