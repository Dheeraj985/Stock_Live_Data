#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 10 16:41:47 2025

@author: dheerajkashyapvaranasi
"""

import subprocess
import time

# Step 1: Start Docker (Kafka, Zookeeper, PostgreSQL)
print("🚀 Starting Docker...")
subprocess.run(["docker", "compose", "up", "-d"])

# Optional: wait a few seconds for Kafka/Postgres to be ready
time.sleep(10)

# Step 2: Start Producer
print("📡 Starting Kafka Producer...")
producer_process = subprocess.Popen(["python", "finnhub_producer.py"])

# Step 3: Start Spark job to write to Postgres
print("🔥 Starting Spark Streaming job...")
spark_process = subprocess.Popen([
    "spark-submit", "--jars", "postgresql-42.7.3.jar", "spark_kafka_alerts.py"
])

# Optionally wait a few seconds and trigger BQ Load
time.sleep(20)
print("📦 Loading to BigQuery...")
subprocess.run(["python", "ETL_schedule.py"])

# Optional: Wait for manual termination
input("Press ENTER to stop everything...")

# Stop all processes
producer_process.terminate()
spark_process.terminate()
