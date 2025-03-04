from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
import psycopg2
import boto3
import json

# Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "data_stream"

# PostgreSQL Configuration
POSTGRES_HOST = "localhost"
POSTGRES_DB = "analytics"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "password"

# AWS S3 Configuration
S3_BUCKET = "your-s3-bucket-name"

# Define Schema for Kafka Data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("value", FloatType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read Stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .load()

# Deserialize JSON messages
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = connection.cursor()
    for row in batch_df.collect():
        cursor.execute("INSERT INTO sensor_data (id, value, timestamp) VALUES (%s, %s, %s)",
                       (row.id, row.value, row.timestamp))
    connection.commit()
    cursor.close()
    connection.close()

df.writeStream.foreachBatch(write_to_postgres).start()

# Write to S3 as Parquet
def write_to_s3(batch_df, batch_id):
    batch_df.write.format("parquet").mode("append").save(f"s3a://{S3_BUCKET}/data/")

df.writeStream.foreachBatch(write_to_s3).start()

# Start the streaming process
df.writeStream.format("console").outputMode("append").start().awaitTermination()
