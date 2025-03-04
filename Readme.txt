# Real-Time Data Pipeline for Streaming Analytics

## Overview
This project is a real-time data processing pipeline using Apache Kafka, Apache Spark Streaming, Python, AWS S3, and PostgreSQL.

## Features
- Real-time data ingestion using **Kafka Producers**.
- Stream processing with **Apache Spark Streaming**.
- Stores data in **PostgreSQL and AWS S3**.
- Uses **Docker Compose** to set up Kafka, Zookeeper, and PostgreSQL.

## Installation

### 1️⃣ Install Dependencies
```sh
pip install pyspark kafka-python psycopg2 boto3
```

### 2️⃣ Start Docker Services
```sh
docker-compose up -d
```

### 3️⃣ Run Kafka Producer
```sh
python kafka_producer/producer.py
```

### 4️⃣ Run Spark Streaming Processor
```sh
python spark_streaming/spark_processor.py
```

## API Endpoints
- **Kafka Producer** → Sends data every 2 seconds to Kafka.
- **Spark Streaming** → Reads from Kafka, processes data, and stores in PostgreSQL/S3.

## Technologies Used
- **Kafka** → Message queue for real-time data ingestion.
- **Spark Streaming** → Real-time processing framework.
- **PostgreSQL** → Relational database for analytics.
- **AWS S3** → Cloud storage for scalable storage.
- **Docker** → Containerized services.

## Author
Shantanu Fuke