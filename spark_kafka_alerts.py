#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, LongType

# 1. Spark Session with PostgreSQL JDBC jar
spark = SparkSession.builder \
    .appName("StockPriceAlerting") \
    .master("local[*]") \
    .config("spark.jars", "postgresql-42.7.6.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define JSON schema from Kafka
schema = StructType() \
    .add("close", FloatType()) \
    .add("high", FloatType()) \
    .add("low", FloatType()) \
    .add("open", FloatType()) \
    .add("pc", FloatType()) \
    .add("t", LongType()) \
    .add("Stock", StringType())

# 3. Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parse JSON from Kafka value
df_parsed = df_raw.selectExpr("CAST(value AS STRING) AS json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# 5. Define PostgreSQL batch writer
def write_to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/stocks") \
            .option("dbtable", "stock_prices") \
            .option("user", "stockuser") \
            .option("password", "pass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} written to database.")
    except Exception as e:
        print(f" Error in batch {batch_id} →", str(e))

# 6. Write Stream to PostgreSQL
query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoints/stock_db") \
    .start()

query.awaitTermination()
