"""
Week 6 - Batch Layer (Lambda Architecture)
Backfill missing dates from Binance API
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, from_unixtime, to_date, year, month, first, last, max, min, 
    sum, count, avg, lit, sequence, explode, lag, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, DoubleType
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# Base directory
base_dir = r"D:\BigDataProject"

# Initialize Spark
spark = SparkSession.builder \
    .appName("Week6_Backfill_BatchLayer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=" * 80)
print("WEEK 6 - BATCH LAYER (Lambda Architecture)")
print("Backfill missing dates from Binance API")
print("=" * 80)

# STEP 1: DETECT LAST DATE
print("\n[STEP 1] Detecting last date in existing data...")

daily_filled_path = os.path.join(base_dir, "data_analysis", "daily_filled")

try:
    df_existing = spark.read.parquet(daily_filled_path)
    last_date_existing = df_existing.agg(F.max("date")).first()[0]
    data_source = "daily_filled"
except:
    try:
        prophet_path = os.path.join(base_dir, "data_analysis", "prophet_input")
        df_existing = spark.read.parquet(prophet_path)
        last_date_existing = df_existing.agg(F.max("ds")).first()[0]
        data_source = "prophet_input"
    except:
        print("  [WARN] No existing data found!")
        spark.stop()
        exit(1)

print(f"  [OK] Last date found in {data_source}: {last_date_existing}")

today = datetime.now().strftime('%Y-%m-%d')
fetch_start_date = (datetime.strptime(str(last_date_existing), '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
gap_days = (datetime.strptime(today, '%Y-%m-%d') - datetime.strptime(fetch_start_date, '%Y-%m-%d')).days

print(f"  [DATE] Today: {today}")
print(f"  [DATA] Gap: {gap_days} days")

if gap_days <= 0:
    print("\n[OK] Data is already up to date! No backfill needed.")
    print("  You can start streaming for real-time data:")
    print("    cd week6_streaming")
    print("    docker-compose up -d")
    print("    python websocket_producer.py")
    print("    python spark_streaming_consumer.py")
    spark.stop()
    exit(0)

print(f"\n  [TARGET] Will backfill: {fetch_start_date} -> {today} ({gap_days} days)")

# STEP 2: FETCH FROM BINANCE API
print("\n[STEP 2] Fetching data from Binance API...")

def fetch_binance_klines(symbol, start_date, end_date, interval='1m'):
    """Fetch klines from Binance API with pagination"""
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000) + 86399999
    
    all_data = []
    current_start = start_ts
    
    while current_start < end_ts:
        url = f"https://api.binance.com/api/v3/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': current_start,
            'endTime': end_ts,
            'limit': 1000
        }
        
        max_retries = 3
        for retry in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                
                all_data.extend(data)
                current_start = int(data[-1][0]) + 60000
                time.sleep(0.1)
                break
            except Exception as e:
                if retry < max_retries - 1:
                    print(f"    Retry after error: {e}")
                    time.sleep(2)
                else:
                    print(f"    [WARN] Failed after {max_retries} retries")
                    break
    
    return all_data

all_klines = []
for symbol in ["BTCUSDT", "ETHUSDT"]:
    print(f"\n  Fetching {symbol}...")
    klines = fetch_binance_klines(symbol, fetch_start_date, today)
    
    if klines:
        for k in klines:
            all_klines.append({
                'timestamp': int(k[0]) // 1000,
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'volume': float(k[5]),
                'symbol': symbol
            })
        print(f"    [OK] Fetched {len(klines):,} rows")
    else:
        print(f"    [WARN] No data fetched")

if not all_klines:
    print("\n[ERROR] No data fetched. Exiting.")
    spark.stop()
    exit(1)

print(f"\n  Total rows fetched: {len(all_klines):,}")

# STEP 3: CLEAN DATA
print("\n[STEP 3] Cleaning data...")

df_raw = spark.createDataFrame(all_klines)
df_raw = df_raw.withColumn("datetime", from_unixtime(col("timestamp")))
df_raw = df_raw.withColumn("date", to_date("datetime"))

# Remove duplicates
df_clean = df_raw.dropDuplicates(["symbol", "timestamp"])
clean_rows = df_clean.count()
print(f"  [OK] After deduplication: {clean_rows:,} rows")

# STEP 4: AGGREGATE TO DAILY OHLC
print("\n[STEP 4] Aggregating to daily OHLC...")

minmax = df_clean.groupBy("symbol", "date").agg(
    F.min("timestamp").alias("min_ts"),
    F.max("timestamp").alias("max_ts")
)

opens = df_clean.join(minmax, on=["symbol", "date"]) \
    .filter(col("timestamp") == col("min_ts")) \
    .select("symbol", "date", col("open").alias("daily_open"))

closes = df_clean.join(minmax, on=["symbol", "date"]) \
    .filter(col("timestamp") == col("max_ts")) \
    .select("symbol", "date", col("close").alias("daily_close"))

basic = df_clean.groupBy("symbol", "date").agg(
    F.max("high").alias("daily_high"),
    F.min("low").alias("daily_low"),
    F.sum("volume").alias("daily_volume")
)

df_daily = basic.join(opens, ["symbol", "date"], "left") \
                .join(closes, ["symbol", "date"], "left") \
                .orderBy("symbol", "date")

daily_count = df_daily.count()
print(f"  [OK] Daily aggregation: {daily_count} rows")
df_daily.groupBy("symbol").agg(
    count("*").alias("days"),
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show(truncate=False)

# STEP 5: FORWARD FILL MISSING DATES
print("\n[STEP 5] Forward filling missing dates...")

date_range_df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{fetch_start_date}'),
        to_date('{today}'),
        interval 1 day
    )) as date
""")

df_filled_list = []

for symbol in ["BTCUSDT", "ETHUSDT"]:
    df_symbol = df_daily.filter(col("symbol") == symbol)
    df_complete = date_range_df.crossJoin(df_symbol.select("symbol").distinct())
    df_with_gaps = df_complete.join(df_symbol, ["symbol", "date"], "left")
    
    window_spec = Window.partitionBy("symbol").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    
    for col_name in ["daily_open", "daily_high", "daily_low", "daily_close", "daily_volume"]:
        df_with_gaps = df_with_gaps.withColumn(
            col_name,
            F.last(col(col_name), ignorenulls=True).over(window_spec)
        )
    
    df_filled_list.append(df_with_gaps)

df_filled = df_filled_list[0]
if len(df_filled_list) > 1:
    df_filled = df_filled.union(df_filled_list[1])

df_filled = df_filled.filter(col("daily_close").isNotNull())
filled_count = df_filled.count()
print(f"  [OK] After forward fill: {filled_count} rows")

# STEP 6: PREPARE BACKFILL DATA
print("\n[STEP 6] Preparing daily OHLCV data...")

df_backfill = df_filled.select("symbol", "date", "daily_open", "daily_high", "daily_low", "daily_close", "daily_volume")

print(f"  [OK] Backfill data prepared: {df_backfill.count()} rows")
df_backfill.groupBy("symbol").agg(
    count("*").alias("rows"),
    min("date").alias("first"),
    max("date").alias("last")
).show(truncate=False)

# STEP 7: MERGE INTO daily_filled + RECALCULATE MA7/MA30
print("\n[STEP 7] Merging with existing daily_filled...")

try:
    df_old_filled = spark.read.parquet(daily_filled_path)
    old_count = df_old_filled.count()
    print(f"  [FOUND] Found existing daily_filled: {old_count:,} rows")
    
    # Select only OHLCV columns (drop old MA7/MA30)
    df_old_filled = df_old_filled.select("symbol", "date", "daily_open", "daily_high", "daily_low", "daily_close", "daily_volume")
    
    # Union and remove duplicates
    df_merged = df_old_filled.union(df_backfill).dropDuplicates(["symbol", "date"]).orderBy("symbol", "date")
    merged_count = df_merged.count()
    print(f"  [MERGE] After merge: {merged_count:,} rows (added {merged_count - old_count} new)")
except Exception as e:
    print(f"  [INFO] No existing daily_filled, creating new ({type(e).__name__})")
    df_merged = df_backfill

# Recalculate MA7/MA30 for ALL data
print("\n  [CALC] Recalculating MA7/MA30 for entire dataset...")

window_ma7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
window_ma30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)

df_merged = df_merged \
    .withColumn("ma7", avg("daily_close").over(window_ma7)) \
    .withColumn("ma30", avg("daily_close").over(window_ma30)) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date"))

# Save daily_filled
df_merged.write.mode("overwrite").partitionBy("symbol", "year", "month").parquet(daily_filled_path)

print(f"  [OK] Saved daily_filled: {df_merged.count():,} rows")
print(f"  [SAVE] Path: {daily_filled_path}")

# STEP 8: EXTRACT prophet_input FROM daily_filled
print("\n[STEP 8] Extracting prophet_input from daily_filled...")

prophet_path = os.path.join(base_dir, "data_analysis", "prophet_input")

# Extract minimal schema: (ds, y, symbol) - NO MA columns
df_prophet = df_merged.select(
    col("date").alias("ds"),
    col("daily_close").alias("y"),
    "symbol"
)

# Save (overwrite - always sync with daily_filled)
df_prophet.write.mode("overwrite").partitionBy("symbol").parquet(prophet_path)

print(f"  [OK] Prophet input extracted: {df_prophet.count():,} rows")
print(f"  [INFO] Schema: (ds, y, symbol) - MA7/MA30 will be joined from daily_filled")
print(f"  [SAVE] Path: {prophet_path}")

# SUMMARY
print("\n" + "=" * 80)
print("[OK] BACKFILL COMPLETE (BATCH LAYER)")
print("=" * 80)

print("\n[DATA] Final Statistics:")
df_merged.groupBy("symbol").agg(
    count("*").alias("total_days"),
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show(truncate=False)

print("\n[DATA] Output:")
print(f"  [OK] data_analysis/daily_filled/   (Backfilled data with MA7/MA30)")
print(f"  [OK] data_analysis/prophet_input/  (Ready for Prophet)")

print("\n[TARGET] Next Steps:")
print("  1. Start Streaming (Speed Layer):")
print("     cd week6_streaming")
print("     docker-compose up -d")
print("     python websocket_producer.py  # Terminal 1")
print("     python spark_streaming_consumer.py  # Terminal 2")
print("")
print("  2. Let streaming run to collect real-time data")
print("")
print("  3. Merge batch + streaming data:")
print("     python week6_merge.py")

spark.stop()
print("\n[DONE] Week 6 Backfill process finished!")
