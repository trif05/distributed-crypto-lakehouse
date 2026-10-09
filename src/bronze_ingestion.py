import json
import requests
from datetime import datetime, timezone
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("CryptoBronzeIngestion").getOrCreate()

# 1. API Extraction (CoinGecko Public API)
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1,
    "sparkline": "false"
}

# Fetch market data with timeout to prevent hanging connections
response = requests.get(API_URL, params=PARAMS, timeout=30)
response.raise_for_status()
raw_data = response.json()

# Append Ingestion Metadata
ingestion_timestamp = datetime.now(timezone.utc).isoformat()
payload = {
    "ingestion_timestamp": ingestion_timestamp,
    "record_count": len(raw_data),
    "data": raw_data
}

# Transition from Single-Node Driver to Distributed RDD -> DataFrame
json_string = json.dumps(payload)
rdd = spark.sparkContext.parallelize([json_string])
df_raw = spark.read.json(rdd) # Spark DataFrame

# 4. Write to DBFS in Delta format (Bronze Table)
BRONZE_PATH = "/FileStore/crypto_lakehouse/bronze_crypto_raw"

df_raw.write.format("delta").mode("append").save(BRONZE_PATH)

print(f"Bronze ingestion completed successfully at {ingestion_timestamp}. Records fetched: {len(raw_data)}")