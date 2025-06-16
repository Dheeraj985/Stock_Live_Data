#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 10 11:34:08 2025

@author: dheerajkashyapvaranasi
"""

from kafka import KafkaProducer
import requests
import json
import time

API_KEY = "d147169r01qrqeasfk10d147169r01qrqeasfk1g"
KAFKA_TOPIC = "stock_prices"
KAFKA_SERVER = "localhost:9092"



# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
def get_all_symbols():
    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={API_KEY}"
    response = requests.get(url)
    data = response.json()
    # Filter out symbols like 'BRK.A' or empty strings
    return [item["symbol"] for item in data if item["symbol"].isalpha()]

def fetch_stock_quote(symbol):
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEY}"
    response = requests.get(url)
    data = response.json()
    data["symbol"] = symbol
    return data

try:
    symbols =get_all_symbols()
    print(f"fetched{len(symbols)} stock symbols from Finhub.")
    print(f"Producing stock prices for {symbols} to topic '{KAFKA_TOPIC}'...")
    while True:
        for symbol in symbols:
            stock_data = fetch_stock_quote(symbol)
            print("Sent:", stock_data)
            print("Sending to Kafka:", stock_data)
            producer.send(KAFKA_TOPIC, value=stock_data)
            time.sleep(1)  # 1 sec delay between each symbol
        print("Sleeping before next round...\n")
        time.sleep(10)  # wait 10 sec before next full round
except KeyboardInterrupt:
    print("\nStopped by user.")
