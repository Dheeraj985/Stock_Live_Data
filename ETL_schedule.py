#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 10 15:51:07 2025

@author: dheerajkashyapvaranasi
"""

import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

# Load data from PostgreSQL
pg_engine = create_engine("postgresql://stockuser:pass@localhost:5432/stocks")
df = pd.read_sql("SELECT * FROM stock_prices", pg_engine)

# Write to BigQuery
client = bigquery.Client()
table_id = "loyal-conduit-406604.stocks.stock_prices"
job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
job.result()

print("Uploaded to BigQuery!")
