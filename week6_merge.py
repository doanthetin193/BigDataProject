"""
================================================================================
WEEK 6 - SERVING LAYER (Lambda Architecture)
================================================================================
Merge Batch Layer (backfill) + Speed Layer (streaming) data.

Usage:
    python week6_merge.py

What it does:
1. Reads batch data from daily_filled (backfill)
2. Reads streaming data from streaming_output_spark/daily
3. Merges both datasets (union + dedup)
4. Computes MA7/MA30 for the merged timeline
5. Saves final unified dataset

This creates a seamless timeline combining historical + real-time data.
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, avg, year
from pyspark.sql.window import Window
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("Week6_Merge_ServingLayer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=" * 80)
print("WEEK 6 - SERVING LAYER (Lambda Architecture)")
print("Merge Batch + Streaming Data")
print("=" * 80)

# ============================================================================
# STEP 1: READ BATCH LAYER DATA
# ============================================================================
print("\n[STEP 1] Reading Batch Layer (backfill data)...")

try:
    df_batch = spark.read.parquet("data_analysis/daily_filled")
    batch_count = df_batch.count()
    print(f"  ✅ Batch data loaded: {batch_count:,} rows")
    
    df_batch.groupBy("symbol").agg(
        count("*").alias("days"),
        min("date").alias("first_date"),
        max("date").alias("last_date")
    ).show(truncate=False)
    
except Exception as e:
    print(f"  ❌ Error: {e}")
    print("  Please run week6_backfill.py first!")
    spark.stop()
    exit(1)

# ============================================================================
# STEP 2: READ SPEED LAYER DATA (STREAMING)
# ============================================================================
print("\n[STEP 2] Reading Speed Layer (streaming data)...")

streaming_path = "streaming_output_spark/daily"

if not os.path.exists(streaming_path):
    print(f"  ⚠️  No streaming data found at {streaming_path}")
    print("  Start streaming first:")
    print("    cd week6_streaming")
    print("    docker-compose up -d")
    print("    python websocket_producer.py")
    print("    python spark_streaming_consumer.py")
    print("\n  Or proceed with batch data only...")
    
    print("\n✅ Using batch data only (no streaming data to merge)")
    spark.stop()
    exit(0)

try:
    df_streaming = spark.read.parquet(streaming_path)
    streaming_count = df_streaming.count()
    print(f"  ✅ Streaming data loaded: {streaming_count:,} rows")
    
    df_streaming.groupBy("symbol").agg(
        count("*").alias("days"),
        min("date").alias("first_date"),
        max("date").alias("last_date")
    ).show(truncate=False)
    
except Exception as e:
    print(f"  ⚠️  Error reading streaming data: {e}")
    print("  Using batch data only...")
    spark.stop()
    exit(0)

# ============================================================================
# STEP 3: ALIGN SCHEMAS AND MERGE
# ============================================================================
print("\n[STEP 3] Merging batch + streaming data...")

# Ensure both have same columns
batch_cols = set(df_batch.columns)
streaming_cols = set(df_streaming.columns)

# Use common columns
common_cols = ["symbol", "date", "open", "high", "low", "close", "volume"]

# Check if we have MA7/MA30 in batch
if "MA7" in batch_cols and "MA30" in batch_cols:
    common_cols.extend(["MA7", "MA30"])

df_batch_aligned = df_batch.select(*common_cols)

# For streaming, select available columns
available_streaming_cols = [c for c in common_cols if c in streaming_cols]
df_streaming_aligned = df_streaming.select(*available_streaming_cols)

# If streaming doesn't have MA7/MA30, add null columns
if "MA7" not in streaming_cols:
    from pyspark.sql.functions import lit
    df_streaming_aligned = df_streaming_aligned.withColumn("MA7", lit(None).cast("double"))
    df_streaming_aligned = df_streaming_aligned.withColumn("MA30", lit(None).cast("double"))

# Union
df_merged = df_batch_aligned.union(df_streaming_aligned)

# Remove duplicates (keep first occurrence)
df_merged = df_merged.dropDuplicates(["symbol", "date"])

# Sort
df_merged = df_merged.orderBy("symbol", "date")

merged_count = df_merged.count()
print(f"  ✅ Merged data: {merged_count:,} rows")

# ============================================================================
# STEP 4: RECOMPUTE MA7/MA30 FOR ENTIRE TIMELINE
# ============================================================================
print("\n[STEP 4] Recomputing MA7/MA30 for merged timeline...")

window_ma7 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)
window_ma30 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)

df_merged = df_merged.withColumn("MA7", avg("close").over(window_ma7))
df_merged = df_merged.withColumn("MA30", avg("close").over(window_ma30))

print(f"  ✅ MA7 and MA30 recomputed")

# ============================================================================
# STEP 5: SAVE UNIFIED DATASET
# ============================================================================
print("\n[STEP 5] Saving unified dataset...")

# Add year for partitioning
df_merged = df_merged.withColumn("year", year("date"))

# Save to daily_filled (overwrite with merged data)
output_path = "data_analysis/daily_filled"
df_merged.write.mode("overwrite").partitionBy("symbol", "year").parquet(output_path)

print(f"  ✅ Saved to {output_path}")

# Update prophet_input
df_prophet = df_merged.select(
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
print("✅ MERGE COMPLETE (SERVING LAYER)")
print("=" * 80)

df_final = spark.read.parquet("data_analysis/daily_filled")
df_final.groupBy("symbol").agg(
    count("*").alias("total_days"),
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show(truncate=False)

print("\n📊 Unified Timeline:")
print(f"  Batch Layer (backfill): Historical data up to streaming start")
print(f"  Speed Layer (streaming): Real-time data from streaming")
print(f"  Serving Layer: Seamless merged timeline")

print("\n🎯 Data ready for:")
print("  - Prophet forecasting (python prophet_train.py)")
print("  - Analysis and visualization")
print("  - Final report presentation")

spark.stop()
