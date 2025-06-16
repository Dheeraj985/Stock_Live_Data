# 📈 Real-Time Stock Price Streaming Pipeline

This repository contains a real-time data pipeline that fetches stock prices using the Finnhub API, sends them to a Kafka topic, processes them using Spark Structured Streaming, stores them in PostgreSQL, and periodically loads them into BigQuery for BI visualization.

---

## 🧩 Components

### 1. Kafka Producer (`finhub_producer.py`)
Fetches real-time stock quotes for predefined symbols using the [Finnhub API](https://finnhub.io/) and sends them as JSON to a Kafka topic `stock_prices`.

**Fields:**
- `c`: Current price  
- `h`: High price of the day  
- `l`: Low price of the day  
- `o`: Open price of the day  
- `pc`: Previous close price  
- `t`: Timestamp  
- `symbol`: Stock symbol  

---

### 2. Kafka & Zookeeper (`docker-compose.yml`)
Uses Docker Compose to spin up:
- Kafka broker (port `9092`)
- Zookeeper service (default port `2181`)

---

### 3. Spark Structured Streaming (`spark_kafka_alerts.py`)
Consumes data from Kafka, parses JSON messages, and writes the stream to PostgreSQL using JDBC.

---

### 4. PostgreSQL Database
Stores the processed data into a table named `stock_prices` with columns:
`c, h, l, o, pc, t, symbol`.

---

### 5. ETL to BigQuery (`ETL_schedule.py`)
Transfers data periodically from PostgreSQL to Google BigQuery for integration with BI tools like Looker.

---

## ⚙️ Setup Instructions

### 1. Start Kafka & Zookeeper
```bash
docker-compose up -d
