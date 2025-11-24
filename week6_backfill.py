"""
================================================================================
WEEK 6 - BATCH LAYER (Lambda Architecture)
================================================================================
Backfill missing dates from Binance API when streaming was not running.

Usage:
    python week6_backfill.py

What it does:
1. Detects last date in existing data (from Week 1-5 pipeline)
2. Fetches gap data from Binance API (1-minute candles)
3. Processes: Clean, Daily OHLC, Forward fill, MA7/MA30
4. Saves to data_analysis/daily_filled/

This complements the Streaming layer (real-time) with Batch layer (historical).
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, from_unixtime, to_date, year, month, dayofmonth,
    first, last, max, min, sum, count, avg, when, lit, 
    expr, sequence, explode, lag, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, DoubleType
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("Week6_Backfill_BatchLayer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=" * 80)
print("WEEK 6 - BATCH LAYER (Lambda Architecture)")
print("Backfill missing dates from Binance API")
print("=" * 80)

# ============================================================================
# STEP 1: DETECT LAST DATE IN EXISTING DATA
# ============================================================================
print("\n[STEP 1] Detecting last date in existing data...")

try:
    # Try reading from daily_filled first (if exists from previous runs)
    df_existing = spark.read.parquet("data_analysis/daily_filled")
    last_date_existing = df_existing.agg(max("date")).collect()[0][0]
    data_source = "daily_filled"
except:
    try:
        # Otherwise read from prophet_input (Week 4 output)
        df_existing = spark.read.parquet("data_analysis/prophet_input")
        last_date_existing = df_existing.agg(max("ds")).collect()[0][0]
        data_source = "prophet_input"
    except:
        print("  ⚠️  No existing data found!")
        print("  Please run Week 1-5 pipeline first:")
        print("    python convert_to_parquet.py")
        print("    python clean_parquet.py")
        print("    python preprocess_step1.py")
        print("    python preprocess_step2.py")
        spark.stop()
        exit(1)

print(f"  ✅ Last date found in {data_source}: {last_date_existing}")

# Calculate gap
today = datetime.now().date()
gap_days = (today - last_date_existing).days

print(f"  📅 Today: {today}")
print(f"  📊 Gap: {gap_days} days")

if gap_days <= 0:
    print("\n✅ Data is already up to date! No backfill needed.")
    print("  You can start streaming for real-time data:")
    print("    cd week6_streaming")
    print("    docker-compose up -d")
    print("    python websocket_producer.py")
    print("    python spark_streaming_consumer.py")
    spark.stop()
    exit(0)

# Fetch from day after last_date
fetch_start_date = last_date_existing + timedelta(days=1)
print(f"\n  🎯 Will backfill: {fetch_start_date} → {today} ({gap_days} days)")

# ============================================================================
# STEP 2: FETCH DATA FROM BINANCE API
# ============================================================================
print("\n[STEP 2] Fetching data from Binance API...")

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
            current_start = klines[-1][6] + 1  # Next start = last close_time + 1ms
            time.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            print(f"    Retry after error: {e}")
            time.sleep(5)
            continue
    
    return all_klines

start_ms = int(datetime.combine(fetch_start_date, datetime.min.time()).timestamp() * 1000)
end_ms = int(datetime.combine(today, datetime.max.time()).timestamp() * 1000)

# Fetch both BTC and ETH
new_data_frames = []

for symbol in ["BTCUSDT", "ETHUSDT"]:
    print(f"\n  Fetching {symbol}...")
    
    klines = fetch_binance_klines(symbol, "1m", start_ms, end_ms)
    
    if not klines:
        print(f"    ⚠️  No data fetched")
        continue
    
    # Convert to pandas DataFrame
    df_klines = pd.DataFrame(klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_volume', 'trades', 'taker_buy_base',
        'taker_buy_quote', 'ignore'
    ])
    
    df_klines['open_time'] = pd.to_datetime(df_klines['open_time'], unit='ms')
    
    for col_name in ['open', 'high', 'low', 'close', 'volume']:
        df_klines[col_name] = df_klines[col_name].astype(float)
    
    # Convert to Spark DataFrame
    df_spark = spark.createDataFrame(df_klines[['open_time', 'open', 'high', 'low', 'close', 'volume']])
    df_spark = df_spark.withColumn("symbol", lit(symbol))
    df_spark = df_spark.withColumn("date", to_date(col("open_time")))
    
    new_data_frames.append(df_spark)
    print(f"    ✅ Fetched {df_spark.count():,} rows")

if not new_data_frames:
    print("\n❌ No data fetched. Exiting.")
    spark.stop()
    exit(1)

# Union BTC + ETH
df_new_raw = new_data_frames[0]
if len(new_data_frames) > 1:
    df_new_raw = df_new_raw.union(new_data_frames[1])

print(f"\n  Total rows fetched: {df_new_raw.count():,}")

# ============================================================================
# STEP 3: CLEAN DATA
# ============================================================================
print("\n[STEP 3] Cleaning data...")

df_new_clean = df_new_raw.dropDuplicates(["symbol", "open_time"])
clean_rows = df_new_clean.count()

print(f"  ✅ After deduplication: {clean_rows:,} rows")

# ============================================================================
# STEP 4: AGGREGATE TO DAILY OHLC
# ============================================================================
print("\n[STEP 4] Aggregating to daily OHLC...")

df_daily = df_new_clean.groupBy("symbol", "date").agg(
    first("open").alias("open"),
    max("high").alias("high"),
    min("low").alias("low"),
    last("close").alias("close"),
    sum("volume").alias("volume")
)

daily_count = df_daily.count()
print(f"  ✅ Daily aggregation: {daily_count} rows")

df_daily.groupBy("symbol").agg(
    count("*").alias("days"),
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show(truncate=False)

# ============================================================================
# STEP 5: FORWARD FILL MISSING DATES
# ============================================================================
print("\n[STEP 5] Forward filling missing dates...")

# Generate complete date range
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
    
    # Cross join with date range
    df_complete = date_range_df.crossJoin(df_symbol.select("symbol").distinct())
    
    # Left join with actual data
    df_with_gaps = df_complete.join(df_symbol, ["symbol", "date"], "left")
    
    # Forward fill using window function
    window_spec = Window.partitionBy("symbol").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    
    for col_name in ["open", "high", "low", "close", "volume"]:
        df_with_gaps = df_with_gaps.withColumn(
            col_name,
            F.last(col(col_name), ignorenulls=True).over(window_spec)
        )
    
    df_filled_list.append(df_with_gaps)

df_filled = df_filled_list[0]
if len(df_filled_list) > 1:
    df_filled = df_filled.union(df_filled_list[1])

# Remove rows where still null (shouldn't happen if data exists)
df_filled = df_filled.filter(col("close").isNotNull())

filled_count = df_filled.count()
print(f"  ✅ After forward fill: {filled_count} rows")

# ============================================================================
# STEP 6: COMPUTE MA7 AND MA30
# ============================================================================
print("\n[STEP 6] Computing MA7 and MA30...")

window_ma7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
window_ma30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)

df_filled = df_filled.withColumn("MA7", avg("close").over(window_ma7))
df_filled = df_filled.withColumn("MA30", avg("close").over(window_ma30))

print(f"  ✅ MA7 and MA30 computed")

# ============================================================================
# STEP 7: SAVE TO daily_filled
# ============================================================================
print("\n[STEP 7] Saving backfill data...")

# Add year column for partitioning
df_filled = df_filled.withColumn("year", year("date"))

# Save to daily_filled
output_path = "data_analysis/daily_filled"
df_filled.write.mode("overwrite").partitionBy("symbol", "year").parquet(output_path)

print(f"  ✅ Saved to {output_path}")
print(f"  Total rows: {df_filled.count():,}")

# ============================================================================
# STEP 8: UPDATE prophet_input
# ============================================================================
print("\n[STEP 8] Updating prophet_input...")

df_prophet = df_filled.select(
    col("date").alias("ds"),
    col("close").alias("y"),
    "symbol",
    "MA7",
    "MA30"
).orderBy("symbol", "ds")

df_prophet.write.mode("overwrite").partitionBy("symbol").parquet("data_analysis/prophet_input")

print(f"  ✅ Prophet input updated")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("✅ BACKFILL COMPLETE (BATCH LAYER)")
print("=" * 80)

df_final = spark.read.parquet("data_analysis/daily_filled")
df_final.groupBy("symbol").agg(
    count("*").alias("total_days"),
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show(truncate=False)

print("\n📊 Output:")
print(f"  ✅ data_analysis/daily_filled/   (Backfilled data with MA7/MA30)")
print(f"  ✅ data_analysis/prophet_input/  (Ready for Prophet)")

print("\n🎯 Next Steps:")
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
