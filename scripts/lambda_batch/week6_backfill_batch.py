"""
Temporary script: Fetch backfill data in 2 batches to avoid timeout
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, lit
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

spark = SparkSession.builder.appName("BackfillBatch").getOrCreate()

def fetch_binance_klines(symbol, interval, start_time, end_time):
    """Fetch historical klines from Binance API."""
    url = "https://api.binance.com/api/v3/klines"
    all_klines = []
    current_start = start_time
    
    while current_start < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_time,
            "limit": 1000
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            klines = response.json()
            
            if not klines:
                break
            
            all_klines.extend(klines)
            current_start = klines[-1][6] + 1
            time.sleep(0.2)  # More conservative rate limit
            
            if len(all_klines) % 10000 == 0:
                print(f"    Progress: {len(all_klines):,} rows fetched...")
            
        except Exception as e:
            print(f"    Error: {e}")
            time.sleep(10)
            continue
    
    return all_klines

# Dates
start_date = datetime(2025, 9, 26).date()
today = datetime.now().date()
total_days = (today - start_date).days

print(f"Total gap: {total_days} days")
print(f"Splitting into 2 batches...")

# Batch 1: First 40 days
batch1_start = start_date
batch1_end = start_date + timedelta(days=40)

print(f"\n=== BATCH 1: {batch1_start} -> {batch1_end} ===")

batch1_data = []
for symbol in ["BTCUSDT", "ETHUSDT"]:
    print(f"\nFetching {symbol}...")
    
    start_ms = int(datetime.combine(batch1_start, datetime.min.time()).timestamp() * 1000)
    end_ms = int(datetime.combine(batch1_end, datetime.max.time()).timestamp() * 1000)
    
    klines = fetch_binance_klines(symbol, "1m", start_ms, end_ms)
    
    if klines:
        df = pd.DataFrame(klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])
        
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        for c in ['open', 'high', 'low', 'close', 'volume']:
            df[c] = df[c].astype(float)
        
        df_spark = spark.createDataFrame(df[['open_time', 'open', 'high', 'low', 'close', 'volume']])
        df_spark = df_spark.withColumn("symbol", lit(symbol))
        df_spark = df_spark.withColumn("date", to_date(col("open_time")))
        
        batch1_data.append(df_spark)
        print(f"  [OK] Fetched {df_spark.count():,} rows")

if batch1_data:
    df_batch1 = batch1_data[0]
    if len(batch1_data) > 1:
        df_batch1 = df_batch1.union(batch1_data[1])
    
    df_batch1.write.mode("overwrite").parquet("data_analysis/temp_batch1.parquet")
    print(f"\n[SAVED] Batch 1: {df_batch1.count():,} rows -> temp_batch1.parquet")

print("\n" + "="*60)
print("BATCH 1 COMPLETE! Wait 30 seconds before Batch 2...")
print("="*60)
time.sleep(30)

# Batch 2: Remaining days
batch2_start = batch1_end + timedelta(days=1)
batch2_end = today

print(f"\n=== BATCH 2: {batch2_start} -> {batch2_end} ===")

batch2_data = []
for symbol in ["BTCUSDT", "ETHUSDT"]:
    print(f"\nFetching {symbol}...")
    
    start_ms = int(datetime.combine(batch2_start, datetime.min.time()).timestamp() * 1000)
    end_ms = int(datetime.combine(batch2_end, datetime.max.time()).timestamp() * 1000)
    
    klines = fetch_binance_klines(symbol, "1m", start_ms, end_ms)
    
    if klines:
        df = pd.DataFrame(klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])
        
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        for c in ['open', 'high', 'low', 'close', 'volume']:
            df[c] = df[c].astype(float)
        
        df_spark = spark.createDataFrame(df[['open_time', 'open', 'high', 'low', 'close', 'volume']])
        df_spark = df_spark.withColumn("symbol", lit(symbol))
        df_spark = df_spark.withColumn("date", to_date(col("open_time")))
        
        batch2_data.append(df_spark)
        print(f"  [OK] Fetched {df_spark.count():,} rows")

if batch2_data:
    df_batch2 = batch2_data[0]
    if len(batch2_data) > 1:
        df_batch2 = df_batch2.union(batch2_data[1])
    
    df_batch2.write.mode("overwrite").parquet("data_analysis/temp_batch2.parquet")
    print(f"\n[SAVED] Batch 2: {df_batch2.count():,} rows -> temp_batch2.parquet")

print("\n" + "="*60)
print("ALL BATCHES COMPLETE!")
print("="*60)
print("\nNext: Run week6_backfill.py to merge temp data into daily_filled")

spark.stop()
